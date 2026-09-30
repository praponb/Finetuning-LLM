from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select("order_id", "status")
    .intersect(
        df_payments
        .filter(F.col("amount") >= 1200)
        .select("order_id", "status")
    )
    .orderBy("status")
)
