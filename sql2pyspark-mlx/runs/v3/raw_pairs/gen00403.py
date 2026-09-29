from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .union(df_shipments.select("cost"))
    .distinct()
)
