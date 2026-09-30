from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("merchant")
    .union(df_sessions.filter(F.col("country").isNotNull()).select("country"))
)
