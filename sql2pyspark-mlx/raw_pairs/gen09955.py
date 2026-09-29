from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.percentile_approx("price", 0.95).alias("price_p95"),
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty"))).alias("sports_total_stock_qty"),
    )
    .filter(F.col("price_p95") >= 0)
    .orderBy(F.col("sports_total_stock_qty").desc_nulls_first())
)
