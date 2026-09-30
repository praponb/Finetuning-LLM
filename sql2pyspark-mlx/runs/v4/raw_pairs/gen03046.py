from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-03-01")
    .select("status", "order_id")
    .unionByName(
        df_payments
        .filter(F.to_date("paid_at") >= "2024-09-09")
        .select("status", "order_id")
    )
)
