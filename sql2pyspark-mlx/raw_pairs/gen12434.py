from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .select("status", "channel")
    .orderBy(F.col("channel").desc())
)
