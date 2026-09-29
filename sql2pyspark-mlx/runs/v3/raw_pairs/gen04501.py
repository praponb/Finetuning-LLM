from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("name").like("%mma%"))
    .select("country")
    .unionByName(df_sessions.select("country"))
    .orderBy(F.col("country").desc())
)
