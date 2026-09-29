from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Par%"))
    .select("country")
    .unionByName(df_sessions.select("country"))
    .orderBy(F.col("country").desc())
)
