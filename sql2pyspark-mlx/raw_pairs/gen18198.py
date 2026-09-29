from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("id", "store_id", "order_date")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("id", "store_id", "order_date")
    )
)
