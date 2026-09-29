from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(50, 65))
    .groupBy("city", "segment")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .filter(F.col("avg_age") < 70)
    .select("city", "segment", "max_credit_limit", "avg_age")
    .orderBy(F.col("max_credit_limit").desc())
)
