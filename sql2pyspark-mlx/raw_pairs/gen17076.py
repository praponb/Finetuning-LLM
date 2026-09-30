from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & ((F.col("browser") == "Edge") | (F.col("browser") != "Safari")))
    .select("id", "customer_id", "country", "device")
    .orderBy("customer_id")
)
