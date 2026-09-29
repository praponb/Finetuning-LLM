from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "TH")
    .select("id", "browser", "customer_id")
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
)
