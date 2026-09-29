from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([12, 26, 33, 40]))
    .select("country", "browser", F.initcap(F.trim("device")).alias("device_title"))
)
