from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["US", "JP", "SG"]) & F.hour("started_at").between(7, 13) & (F.col("country") != "UK"))
    .orderBy(F.col("started_at").desc())
)
