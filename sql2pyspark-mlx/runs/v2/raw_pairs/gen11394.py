from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 14))
    .select("id", "method")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([21, 27, 31]))
        .select("id", "method")
    )
)
