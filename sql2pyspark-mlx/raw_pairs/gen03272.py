from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("device_prefix", F.substring("device", 1, 2))
    .withColumn("device_group", F.when(F.col("device").isin(["desktop", "mobile"]), "Core").otherwise("Other"))
)
