from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([22, 25, 26, 37]))
    .select("page")
    .unionByName(df_web_events.filter(F.col("page") == "/cart").select("page"))
    .distinct()
)
