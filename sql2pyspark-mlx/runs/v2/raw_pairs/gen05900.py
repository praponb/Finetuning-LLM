from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("id", "status", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("order_id").isin([2, 4, 25]))
        .select("id", "status", "order_id")
    )
    .distinct()
    .orderBy("order_id")
)
