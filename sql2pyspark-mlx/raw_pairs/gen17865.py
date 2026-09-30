from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("method", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select("method", "order_id")
    )
    .distinct()
)
