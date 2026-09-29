from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select("id", "event_type")
    .subtract(
        df_web_events
        .filter(~F.col("page").isin(["/product", "/checkout"]))
        .select("id", "event_type")
    )
)
