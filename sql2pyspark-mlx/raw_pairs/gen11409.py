from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 19) & F.col("browser").isin(["Chrome", "Edge"]))
    .withColumn("device_country_key", F.concat_ws("/", "device", "country"))
)
