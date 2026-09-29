from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("country") == "US") & F.col("is_active")) | (F.length("name") >= 12))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("avg_age") < 45)
    .orderBy("city")
)
