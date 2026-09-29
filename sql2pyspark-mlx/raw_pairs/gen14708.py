from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Firefox", "Edge"]) & (F.col("customer_id").isNotNull() | F.col("customer_id").isNotNull()))
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("country_length", F.length("country"))
)
