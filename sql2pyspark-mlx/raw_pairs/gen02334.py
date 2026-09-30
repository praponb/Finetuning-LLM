from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(df_sessions.select("country"))
    .distinct()
)
