from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("pct_de") >= 62)
    .orderBy("city")
)
