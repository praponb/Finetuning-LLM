from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 10) & ((F.col("browser") == "Safari") | F.hour("started_at").between(9, 14)))
    .withColumn("country_prefix", F.substring("country", 1, 2))
)
