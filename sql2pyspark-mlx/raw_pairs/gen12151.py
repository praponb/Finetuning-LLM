from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & ((F.col("customer_id") == 38) | F.col("customer_id").isNotNull()))
    .select("id", "started_at", F.initcap(F.trim("country")).alias("country_title"))
)
