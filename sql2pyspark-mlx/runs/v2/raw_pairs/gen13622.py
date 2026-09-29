from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 30)
    .select("status")
    .unionByName(df_shipments.filter(F.col("order_id").isNotNull()).select("status"))
    .distinct()
    .orderBy("status")
)
