from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop"]) & (F.col("customer_id").isNotNull() | ~F.col("country").isin(["SG", "JP", "UK"])))
    .select("id", "customer_id", "country")
)
