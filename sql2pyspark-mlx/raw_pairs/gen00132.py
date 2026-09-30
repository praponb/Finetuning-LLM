from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("amount")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("unit_price")
    )
)
