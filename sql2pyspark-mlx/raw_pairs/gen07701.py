from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("kind"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("kind"), "status"))
    .distinct()
    .orderBy(F.col("kind").desc())
)
