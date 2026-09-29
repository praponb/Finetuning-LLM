from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([14, 16, 18]))
    .select("country")
    .unionByName(df_sessions.filter(F.col("device") == "desktop").select("country"))
    .orderBy("country")
)
