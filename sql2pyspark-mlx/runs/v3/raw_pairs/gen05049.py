from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([22, 23, 28, 39]))
    .select("id", "order_date")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee").between(23, 35))
        .select("id", "order_date")
    )
    .orderBy("order_date")
)
