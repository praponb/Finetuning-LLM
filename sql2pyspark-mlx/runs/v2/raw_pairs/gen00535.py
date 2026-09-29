from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "cash"]))
    .select(
        "id",
        "paid_at",
        F.upper("method").alias("method_upper"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)
