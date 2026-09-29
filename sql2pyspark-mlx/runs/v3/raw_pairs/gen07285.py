from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_payments.select("status", F.lit("payment").alias("source")))
    .distinct()
    .orderBy(F.col("source").desc())
)
