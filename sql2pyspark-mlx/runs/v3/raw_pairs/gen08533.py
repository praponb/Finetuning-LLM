from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("status")
    .intersect(df_shipments.select("status"))
)
