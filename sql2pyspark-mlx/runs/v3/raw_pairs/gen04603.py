from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("avg_age") >= 64)
    .select("segment", "age_range", "avg_age")
    .orderBy(F.col("segment").desc())
)
