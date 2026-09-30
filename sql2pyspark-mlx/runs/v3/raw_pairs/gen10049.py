from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") >= 6)
    .groupBy("is_active")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum("age").alias("total_age"),
    )
    .filter(F.col("total_age") > 170)
    .drop("total_age")
    .orderBy(F.col("min_credit_limit").desc())
)
