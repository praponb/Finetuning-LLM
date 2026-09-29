from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(((F.col("started_at") < "2024-01-01") & F.hour("started_at").between(12, 16)) | F.col("customer_id").isNull()))
    .select("id", "country")
)
