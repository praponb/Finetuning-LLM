from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 13))
    .select("browser", "country")
    .distinct()
    .orderBy(F.col("country").desc(), "browser")
    .limit(5)
)
