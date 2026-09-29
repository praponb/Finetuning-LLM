from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select("id", "order_id", "status")
    .intersect(
        df_payments
        .filter(F.col("amount") < 1350)
        .select("id", "order_id", "status")
    )
)
