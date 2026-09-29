from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("category").orderBy(F.col("quantity").desc(), "id")
w_ordered = Window.orderBy("unit_price_plus_unit_price_rounded", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_order_items
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims = (
    latest
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.category"))
)

latest_order_items = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "unit_price")
)

cleaned = (
    latest_order_items
    .filter(F.col("unit_price") < 250)
    .select(
        "*",
        F.round("unit_price", 2).alias("unit_price_rounded"),
        F.when(F.col("unit_price") >= 340, "High").when(F.col("unit_price") >= 150, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

cleaned_order_items = (
    cleaned
    .filter(F.col("unit_price").between(110, 190))
    .withColumn("unit_price_rounded_per_unit_price", F.round((F.col("unit_price_rounded") / F.col("unit_price")), 2))
    .withColumn("unit_price_plus_unit_price_rounded", (F.col("unit_price") + F.col("unit_price_rounded")))
)

result = (
    cleaned_order_items
    .select(
        "id",
        "unit_price",
        "unit_price_rounded",
        "unit_price_rounded_per_unit_price",
        F.sum("unit_price_rounded").over(w_ordered).alias("last7_sum_unit_price_rounded"),
        F.avg("unit_price_rounded").over(w_ordered).alias("last7_avg_unit_price_rounded"),
        F.count("unit_price_rounded").over(w_ordered).alias("last7_count_unit_price_rounded"),
        F.min("unit_price_rounded").over(w_ordered).alias("last7_min_unit_price_rounded"),
        F.sum("unit_price_rounded").over(w4).alias("cumulative_unit_price_rounded"),
    )
)
