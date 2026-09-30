from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 940)
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([12, 15, 16, 21]))
        .select("status")
    )
)
