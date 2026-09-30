from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "country")
    .orderBy("country", "id")
    .limit(10)
)
