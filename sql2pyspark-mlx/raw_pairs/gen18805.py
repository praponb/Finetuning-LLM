from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["bank_transfer", "paypal"]))
    .select(
        "id",
        "paid_at",
        "amount",
        "order_id",
        F.upper("status").alias("status_upper"),
        F.when(F.col("method").isin(["paypal", "cash"]), "Group A").otherwise("Other").alias("method_group"),
    )
)
