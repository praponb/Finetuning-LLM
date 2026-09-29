from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "cash", "bank_transfer"]) | F.col("method").isin(["card", "promptpay", "bank_transfer"]))
    .select(
        "id",
        "order_id",
        "status",
        F.initcap(F.trim("method")).alias("method_title"),
    )
)
