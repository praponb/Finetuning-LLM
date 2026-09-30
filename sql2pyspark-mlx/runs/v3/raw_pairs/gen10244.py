from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 480)
    .select("id", "order_id", "status")
    .unionByName(
        df_payments
        .filter(F.col("method") == "card")
        .select("id", "order_id", "status")
    )
    .orderBy("id")
)
