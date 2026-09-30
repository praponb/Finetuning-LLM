from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([2, 4, 16, 30]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
