from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "bank_transfer"]) & (F.col("amount") > 770))
    .select(
        "id",
        "paid_at",
        "amount",
        F.concat_ws("/", "status", "method").alias("status_method_key"),
    )
)
