from pyspark.sql import functions as F

result = (
    df_sessions
    .select("device")
    .distinct()
    .orderBy(F.col("device").desc())
    .limit(5)
)
