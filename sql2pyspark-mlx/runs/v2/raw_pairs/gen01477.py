from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "cash")
    .select("id", "status", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("id", "status", "order_id")
    )
    .orderBy("status")
)
