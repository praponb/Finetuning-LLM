from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "country", F.initcap(F.trim("country")).alias("country_title"))
    .orderBy(F.col("country_title").desc(), F.col("id").desc())
    .limit(3)
)
