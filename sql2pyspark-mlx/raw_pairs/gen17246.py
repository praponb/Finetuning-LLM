from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("last_restock_month_end", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-05-14").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.count("quantity").over(w_recent).alias("last3_count_quantity"),
        F.sum("quantity").over(w_recent).alias("last3_sum_quantity"),
        F.avg("quantity").over(w_recent).alias("last3_avg_quantity"),
        F.max("quantity").over(w_recent).alias("last3_max_quantity"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        "a.last_restock_month_end",
        "a.last3_sum_quantity",
        "a.cumulative_quantity",
        F.col("b.city"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .filter(F.col("b.sqft").between(6756, 14764))
    .select(
        "a.*",
        F.col("b.store_name"),
        F.col("b.city").alias("store_city"),
        F.col("b.region"),
    )
)

grouped = (
    enriched
    .groupBy("product_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count(F.when(F.col("store_city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.sum(F.when(F.col("store_city") == "Berlin", F.col("cumulative_quantity")).otherwise(0)).alias("berlin_cumulative_quantity"),
        F.avg("cumulative_quantity").alias("avg_cumulative_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cumulative_quantity", [0.25, 0.5, 0.75]).alias("cumulative_quantity_quartiles"),
    )
)

result = (
    grouped
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "chiang_mai_count",
        "berlin_cumulative_quantity",
        "cumulative_quantity_quartiles",
        F.col("sum_inventory_quantity"),
    )
)
