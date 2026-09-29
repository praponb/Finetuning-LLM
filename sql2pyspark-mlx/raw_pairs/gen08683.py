from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull() & F.col("is_active"))
    .groupBy("segment")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
    .orderBy(F.col("pct_consumer").desc())
)
