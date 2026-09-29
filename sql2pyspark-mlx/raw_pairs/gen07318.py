from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 15900, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("credit_limit_per_london"),
    )
    .orderBy(F.col("credit_limit_per_london").desc())
)
