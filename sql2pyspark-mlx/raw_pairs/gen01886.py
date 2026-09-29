from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("inventory_quantity").desc(), "id")
w3 = Window.partitionBy("category").orderBy("row_num", "id")
w_part = Window.partitionBy("supplier_id")
w5 = Window.partitionBy("supplier_id").orderBy("supplier_id_min_price", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("supplier_id").orderBy(F.col("supplier_id_min_inventory_quantity").desc_nulls_first())
w7 = Window.partitionBy("supplier_id").orderBy("next_inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("supplier_id").orderBy("next_inventory_quantity_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.size("tags").alias("tags_count"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.supplier_id",
        "s.brand_filled",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

deduped = (
    with_last
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .filter(F.col("supplier_id").isNull())
    .select(
        "id",
        "supplier_id",
        "row_num",
        F.lead("inventory_quantity", 2).over(w3).alias("next_inventory_quantity"),
        F.min("price").over(w_part).alias("supplier_id_min_price"),
        F.min("inventory_quantity").over(w_part).alias("supplier_id_min_inventory_quantity"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "supplier_id",
        "row_num",
        "supplier_id_min_price",
        F.avg("next_inventory_quantity").over(w5).alias("next_inventory_quantity_avg_5_prior"),
        F.percent_rank().over(w6).alias("supplier_id_min_inventory_quantity_percent_rank"),
        F.sum("supplier_id_min_inventory_quantity").over(w7).alias("running_supplier_id_min_inventory_quantity"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "row_num",
        "supplier_id_min_price",
        "next_inventory_quantity_avg_5_prior",
        "supplier_id_min_inventory_quantity_percent_rank",
        "running_supplier_id_min_inventory_quantity",
        F.max("running_supplier_id_min_inventory_quantity").over(w_recent).alias("running_max_running_supplier_id_min_inventory_quantity"),
        F.count("running_supplier_id_min_inventory_quantity").over(w_recent).alias("running_count_running_supplier_id_min_inventory_quantity"),
        F.sum("running_supplier_id_min_inventory_quantity").over(w_recent).alias("running_sum_running_supplier_id_min_inventory_quantity"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("supplier_id_min_price") >= 1191, "PREMIUM").otherwise("STANDARD").alias("supplier_id_min_price_tier"),
    )
    .agg(
        F.percentile_approx("supplier_id_min_price", 0.9).alias("supplier_id_min_price_p90"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("supplier_id_min_price_p90").desc())
)
