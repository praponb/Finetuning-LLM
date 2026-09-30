from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop", "mobile"]) & (F.col("country") != "SG"))
    .select("id", "country", "customer_id")
)
