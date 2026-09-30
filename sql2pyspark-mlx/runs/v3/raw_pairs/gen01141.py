from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(10, 13) & F.col("country").isin(["UK", "SG", "DE"]) & F.col("customer_id").isNotNull())
    .select("id", "country", "customer_id")
)
