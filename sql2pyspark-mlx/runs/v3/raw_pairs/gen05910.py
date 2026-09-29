from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .union(df_order_items.select("unit_price"))
)
