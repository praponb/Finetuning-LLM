from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", "browser")
    .distinct()
)
