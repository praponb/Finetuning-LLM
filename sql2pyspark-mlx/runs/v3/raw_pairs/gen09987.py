from pyspark.sql import functions as F

result = (
    df_sessions
    .select("device", "browser")
    .orderBy("device", "browser")
)
