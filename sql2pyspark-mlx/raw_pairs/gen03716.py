from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(490, 930) | F.col("order_id").isNotNull())
    .select(
        "order_id",
        "amount",
        F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "Core").otherwise("Other").alias("status_group"),
        F.dayofweek("paid_at").alias("paid_at_dayofweek"),
    )
)
