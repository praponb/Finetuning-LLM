from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "order_date", "shipping_fee", "store_id")
    .orderBy(F.col("store_id").desc(), "id")
    .limit(50)
)
