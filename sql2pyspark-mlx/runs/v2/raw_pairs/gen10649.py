from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2023-05-01")
    .select("status")
    .unionByName(df_shipments.select("status"))
)
