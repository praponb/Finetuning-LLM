from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", "city")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("credit_limit")).otherwise(0)).alias("berlin_credit_limit"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .filter(F.col("berlin_credit_limit") > 49)
    .select("country", "city", "berlin_credit_limit", "avg_age")
)
