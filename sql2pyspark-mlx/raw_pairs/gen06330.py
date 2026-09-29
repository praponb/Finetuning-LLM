from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", "browser")
    .distinct()
    .orderBy("browser", "country")
    .limit(3)
)
