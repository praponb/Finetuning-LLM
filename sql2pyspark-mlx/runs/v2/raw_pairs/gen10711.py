from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.max("age").alias("max_age"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .orderBy(F.col("signup_quarter").desc(), F.col("max_age").desc())
)
