from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", "browser")
    .distinct()
    .orderBy(F.col("browser").desc(), "country")
    .limit(5)
)
