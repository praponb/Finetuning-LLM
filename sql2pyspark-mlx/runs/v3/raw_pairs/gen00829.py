from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "SHIPPED"]))
    .select("channel", "status")
    .distinct()
    .orderBy("status", "channel")
    .limit(20)
)
