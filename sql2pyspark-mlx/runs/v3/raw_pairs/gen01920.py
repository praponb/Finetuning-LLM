from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page")
    .distinct()
    .orderBy(F.col("page").desc())
    .limit(5)
)
