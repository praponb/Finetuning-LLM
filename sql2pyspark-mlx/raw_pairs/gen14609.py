from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([6, 20, 32, 33]))
    .select("status")
    .subtract(
        df_payments
        .filter(~F.col("method").isin(["card", "promptpay"]))
        .select("status")
    )
)
