from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("merchant")
    .union(df_sessions.select("device"))
    .distinct()
    .orderBy("merchant")
)
