from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "promptpay"]))
    .select("id", "status", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("amount") > 1120)
        .select("id", "status", "order_id")
    )
    .distinct()
)
