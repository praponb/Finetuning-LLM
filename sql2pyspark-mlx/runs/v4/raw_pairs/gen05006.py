from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w_hist = Window.partitionBy("city", "product_id")

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.opened_date"))
)

joined_store_sales = (
    joined
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.*",
        F.col("s.store_name"),
        F.col("s.opened_date").alias("store_opened_date"),
        F.col("s.city"),
    )
)

with_windows = (
    joined_store_sales
    .withColumn("product_id_count_revenue", F.count("revenue").over(w))
    .withColumn("city_count_revenue", F.count("revenue").over(w_hist))
)

summary = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.round(F.avg("city_count_revenue"), 2).alias("avg_city_count_revenue"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.sum(F.when(F.col("city") == "New York", F.col("product_id_count_revenue")).otherwise(0)).alias("new_york_product_id_count_revenue"),
    )
)

result = (
    summary
    .select("store_id", "avg_city_count_revenue", "new_york_product_id_count_revenue")
)
