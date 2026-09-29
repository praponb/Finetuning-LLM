from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc(), "id")
w2 = Window.orderBy("stock_qty", "id")

scored = (
    df_products
    .withColumn("price_bucket", F.ntile(5).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

result = (
    scored
    .groupBy(
        F.when(F.col("price_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("price_bucket_tier"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("price_bucket")).otherwise(0)).alias("globex_price_bucket"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
    )
)
