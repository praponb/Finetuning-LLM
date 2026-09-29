from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "JP") | F.hour("started_at").between(11, 19))
    .select("id", "started_at", "customer_id")
    .orderBy(F.col("customer_id").desc(), F.col("id").desc())
    .limit(1)
)
