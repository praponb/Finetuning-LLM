from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select("browser")
    .unionByName(df_sessions.filter(F.col("customer_id") == 34).select("browser"))
    .orderBy("browser")
)
