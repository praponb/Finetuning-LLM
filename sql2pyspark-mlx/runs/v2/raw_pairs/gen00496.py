from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull())
    .select("page")
    .subtract(df_web_events.filter(F.col("page") == "/home").select("page"))
)
