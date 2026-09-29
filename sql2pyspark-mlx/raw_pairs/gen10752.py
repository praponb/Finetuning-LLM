from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel", "status")
    .distinct()
    .orderBy("status", "channel")
    .limit(5)
)
