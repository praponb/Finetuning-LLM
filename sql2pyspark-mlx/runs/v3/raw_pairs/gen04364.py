from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(df_shipments.filter(F.col("status") == "IN_TRANSIT").select("status"))
)
