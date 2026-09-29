from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([25, 31, 35, 39]))
    .select("id", "order_id", "method")
    .unionByName(
        df_payments
        .filter(F.to_date("paid_at") >= "2025-05-01")
        .select("id", "order_id", "method")
    )
)
