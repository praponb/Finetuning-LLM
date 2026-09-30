from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 700)
    .select("order_id", "status")
    .unionByName(
        df_payments
        .filter(F.col("paid_at") < "2025-05-05")
        .select("order_id", "status")
    )
    .distinct()
    .orderBy("status")
)
