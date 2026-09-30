from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("segment") == "Corporate") | F.col("age").isNotNull())
    .withColumn("name_title", F.initcap(F.trim("name")))
)

customers_stats = (
    base
    .groupBy("country", "is_active")
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
)

result = (
    customers_stats
    .select("country", "is_active", "credit_limit_range", "avg_credit_limit")
    .orderBy(F.col("avg_credit_limit").desc(), "country", "is_active")
    .limit(1)
)
