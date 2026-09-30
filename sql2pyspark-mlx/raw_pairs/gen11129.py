from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

result = (
    with_latest
    .groupBy(F.date_trunc("week", "product_launch_date").alias("week_start"))
    .agg(
        F.max(F.when(F.col("city") == "Tokyo", F.col("product_stock_qty"))).alias("tokyo_max_product_stock_qty"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("city") == "London", F.col("product_stock_qty")).otherwise(0)).alias("london_product_stock_qty"),
    )
    .filter(F.col("num_cities") < 1000)
)
