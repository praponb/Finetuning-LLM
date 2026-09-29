from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(1350, 1510))
    .select(F.col("channel").alias("label"))
    .unionByName(df_employees.select(F.col("full_name").alias("label")))
    .distinct()
)
