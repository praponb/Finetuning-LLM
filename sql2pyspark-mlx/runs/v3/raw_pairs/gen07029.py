from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("city").isin(["Chiang Mai", "Singapore"]) & (F.lower(F.trim("name")) == "kanya srisuk"))
    .withColumn("greatest_credit_limit_age", F.greatest("credit_limit", "age"))
)

agg = (
    base
    .groupBy("city")
    .agg(
        F.count("age").alias("age_count"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

result = (
    agg
    .select("city", "credit_limit_range")
    .orderBy(F.col("city").desc())
)
