from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("we.page"),
        F.initcap(F.trim(F.col("s.browser"))).alias("browser_title"),
    )
    .orderBy("page")
)
