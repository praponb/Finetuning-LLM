from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("status")
    .unionByName(df_shipments.filter(F.col("order_id").isNull()).select("status"))
)
