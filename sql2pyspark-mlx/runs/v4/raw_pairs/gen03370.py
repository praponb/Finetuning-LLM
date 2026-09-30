from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.max("age").alias("max_age"),
    )
    .orderBy("max_age")
)
