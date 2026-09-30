from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") != "tablet") & F.col("country").isin(["JP", "DE", "TH"]))
    .select("id", "customer_id", "device")
    .orderBy(F.col("customer_id").desc(), "device")
)
