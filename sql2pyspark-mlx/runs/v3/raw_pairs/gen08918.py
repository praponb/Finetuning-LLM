from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(df_shipments.select("status"))
    .orderBy("status")
)
