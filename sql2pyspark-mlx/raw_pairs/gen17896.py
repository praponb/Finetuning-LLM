from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("launch_date").desc_nulls_last())
w_part = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("quantity").desc_nulls_last())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.launch_date"))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .filter(F.col("unit_price") >= 200)
    .select(
        "*",
        F.min("quantity").over(w_part).alias("running_min_quantity"),
        F.count("quantity").over(w_part).alias("running_count_quantity"),
        F.max("quantity").over(w_part).alias("running_max_quantity"),
        F.avg("quantity").over(w_part).alias("running_avg_quantity"),
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
    )
)

valid = (
    scored
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([9, 26]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "rank_in_group",
        "running_min_quantity",
        "running_count_quantity",
        "running_max_quantity",
        "quantity_percent_rank",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
)

remaining_keys = (
    valid
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([3, 26]))
        .select("id", "product_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "rank_in_group",
        "running_min_quantity",
        "running_count_quantity",
        "running_max_quantity",
        "quantity_percent_rank",
        F.col("max_inventory_quantity"),
    )
    .orderBy("rank_in_group")
)
