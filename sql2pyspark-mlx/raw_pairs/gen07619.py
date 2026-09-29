from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 37)
    .select("shipping_fee")
    .union(df_shipments.filter(F.col("cost") > 29).select("cost"))
    .distinct()
)
