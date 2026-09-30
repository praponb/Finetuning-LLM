from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1520)
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("status", F.lit("payment").alias("source"))
    )
)
