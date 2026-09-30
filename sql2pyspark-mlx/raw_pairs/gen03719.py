from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "page")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec") > 233)
        .select("id", "page")
    )
)
