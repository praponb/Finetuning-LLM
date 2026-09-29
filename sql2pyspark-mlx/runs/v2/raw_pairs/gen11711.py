from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("age") == 63) & F.col("name").like("Ali%"))
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
)

customers_stats = (
    src
    .groupBy("segment")
    .agg(
        F.round(F.avg("age_plus_credit_limit"), 2).alias("avg_age_plus_credit_limit"),
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)

result = customers_stats
