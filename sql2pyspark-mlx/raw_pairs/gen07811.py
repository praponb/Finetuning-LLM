from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") < "2023-07-01") & (F.col("customer_id").isin([15, 34]) | F.col("customer_id").isin([3, 11, 17])))
    .select("id", "device")
)
