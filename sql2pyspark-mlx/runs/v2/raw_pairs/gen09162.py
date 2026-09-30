from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .groupBy("country")
    .agg(F.sum("age").alias("total_age"))
    .filter(F.col("total_age") > 360)
)
