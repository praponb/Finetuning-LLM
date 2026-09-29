from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") < 0.17) & (F.col("status") != "RETURNED"))
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("channel_lower", F.lower("channel"))
)
