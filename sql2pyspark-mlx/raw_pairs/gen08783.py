from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").asc_nulls_last(), "id")
w_grp = Window.partitionBy("product_id").orderBy("adjusted_quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("adjusted_quantity", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("product_id").orderBy(F.col("adjusted_quantity").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("next_adjusted_quantity", "id").rowsBetween(-2, -1)
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6, 9]))
    .select(
        "id",
        "product_id",
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "adjusted_quantity")
)

with_windows = (
    ranked
    .withColumn("next_adjusted_quantity", F.lead("adjusted_quantity").over(w_grp))
    .withColumn("prev_adjusted_quantity", F.lag("adjusted_quantity").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("adjusted_quantity_avg_6_prior", F.avg("adjusted_quantity").over(w3))
)

enriched = (
    with_windows
    .filter(F.col("adjusted_quantity").between(22, 65))
    .select(
        "id",
        "product_id",
        "next_adjusted_quantity",
        F.row_number().over(w4).alias("seq_no_2"),
        F.avg("prev_adjusted_quantity").over(w5).alias("prev_adjusted_quantity_avg_2_prior"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.next_adjusted_quantity",
        "s.seq_no_2",
        "s.prev_adjusted_quantity_avg_2_prior",
        F.col("b.product_name"),
    )
)

summary = (
    joined
    .groupBy("product_id")
    .agg(
        F.max("prev_adjusted_quantity_avg_2_prior").alias("max_prev_adjusted_quantity_avg_2_prior"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_adjusted_quantity", 0.9).alias("next_adjusted_quantity_p90"),
    )
)

result = (
    summary
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "distinct_ids",
        "next_adjusted_quantity_p90",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("next_adjusted_quantity_p90").desc(), "distinct_ids")
)
