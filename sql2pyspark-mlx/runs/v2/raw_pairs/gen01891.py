from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 14))
    .orderBy("device", F.col("id").desc())
)
