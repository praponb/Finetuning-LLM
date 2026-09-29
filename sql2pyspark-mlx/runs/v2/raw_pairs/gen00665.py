from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Chai%"))
    .select("country")
    .unionByName(df_sessions.filter(F.col("browser").isNotNull()).select("country"))
)
