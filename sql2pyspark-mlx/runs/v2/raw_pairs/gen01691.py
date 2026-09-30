from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop"]))
    .select("id", "country")
    .orderBy(F.col("id").desc())
    .limit(20)
)
