from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select("id", "store_id", "shipping_fee", "order_date")
    .orderBy(F.col("store_id").desc(), "id")
    .limit(3)
)
