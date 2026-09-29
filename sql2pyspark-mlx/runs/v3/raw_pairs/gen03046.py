from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "mobile", "tablet"]) & F.col("browser").isin(["Firefox", "Chrome"]))
    .orderBy("customer_id", "id")
    .limit(50)
)
