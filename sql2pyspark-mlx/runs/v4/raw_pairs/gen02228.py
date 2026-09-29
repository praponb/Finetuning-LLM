from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home"]))
    .groupBy(
        F.when(F.col("duration_sec") >= 488, "high").when(F.col("duration_sec") >= 106, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .filter(F.col("checkout_count") > 5)
    .drop("checkout_count")
)
