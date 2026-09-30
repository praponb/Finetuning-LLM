from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "TH") & F.col("customer_id").isNotNull())
    .select("device", "browser")
)
