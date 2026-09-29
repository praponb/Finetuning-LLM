from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(df_sessions.filter(F.col("device") == "desktop").select("country"))
    .distinct()
    .orderBy("country")
)
