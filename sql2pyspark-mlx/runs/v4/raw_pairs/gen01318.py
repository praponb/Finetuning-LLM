from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([11, 13, 27]))
    .select("order_id")
    .unionByName(df_shipments.filter(F.col("order_id").isNotNull()).select("order_id"))
    .distinct()
)
