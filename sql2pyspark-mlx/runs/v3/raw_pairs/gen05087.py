from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([16, 20, 32]))
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_payments.select("status", F.lit("payment").alias("source")))
)
