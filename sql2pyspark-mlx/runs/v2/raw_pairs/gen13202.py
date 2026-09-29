from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%vy@%"))
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("age")).otherwise(0)).alias("th_age"),
    )
    .filter(F.col("th_age") > 39)
    .orderBy("segment")
)
