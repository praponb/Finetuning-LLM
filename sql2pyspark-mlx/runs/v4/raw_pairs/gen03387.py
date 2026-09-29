from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", "order_id")
    .intersectAll(
        df_payments
        .filter(F.col("paid_at") < "2023-09-01")
        .select("status", "order_id")
    )
)
