from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("customer_id", "order_date")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee").isNotNull())
        .select("customer_id", "order_date")
    )
)
