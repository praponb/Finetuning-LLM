from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.when(F.col("price") >= 1080, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
)
