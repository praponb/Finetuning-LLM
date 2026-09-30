from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1150)
    .select(F.col("status").alias("label"))
    .intersect(df_customers.select(F.col("segment").alias("label")))
)
