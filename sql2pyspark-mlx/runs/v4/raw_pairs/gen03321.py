from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("status")
    .intersect(df_shipments.filter(F.col("cost").isNotNull()).select("status"))
)
