from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 34)
    .select("id", "order_id", "status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([22, 40]))
        .select("id", "order_id", "status")
    )
    .orderBy("id")
)
