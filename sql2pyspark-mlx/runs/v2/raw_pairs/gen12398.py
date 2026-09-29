from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_payments
        .filter(F.hour("paid_at").between(8, 16))
        .select("status", F.lit("payment").alias("source"))
    )
    .orderBy(F.col("status").desc())
)
