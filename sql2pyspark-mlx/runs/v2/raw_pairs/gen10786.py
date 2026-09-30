from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("country")
    .unionByName(df_sessions.select("country"))
)
