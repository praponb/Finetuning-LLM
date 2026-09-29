from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .intersect(df_orders.select("amount"))
)
