from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .union(
        df_order_items
        .filter(F.col("order_id").isin([5, 18, 19, 31]))
        .select("unit_price")
    )
    .distinct()
    .orderBy("amount")
)
