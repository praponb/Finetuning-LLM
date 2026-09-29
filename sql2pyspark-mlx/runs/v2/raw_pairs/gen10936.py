from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select("id", "country")
    .subtract(df_sessions.filter(F.col("customer_id") == 33).select("id", "country"))
    .orderBy("country")
)
