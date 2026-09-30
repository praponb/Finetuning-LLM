from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-02-27")
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([14, 18, 24, 40]))
        .select("status")
    )
    .distinct()
)
