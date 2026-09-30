from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Edge") & (F.to_date("started_at") < "2025-09-01"))
    .select("id", "browser", "customer_id")
    .orderBy("browser", F.col("customer_id").desc())
)
