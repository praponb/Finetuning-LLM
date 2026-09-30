from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 16))
    .select("order_id")
    .unionByName(
        df_payments
        .filter(F.col("amount").between(920, 1390))
        .select("order_id")
    )
)
