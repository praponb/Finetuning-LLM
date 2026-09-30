from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNull())
    .select("method", "status")
    .intersect(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("method", "status")
    )
    .orderBy("status")
)
