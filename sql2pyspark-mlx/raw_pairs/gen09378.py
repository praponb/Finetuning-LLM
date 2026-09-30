from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page")
    .subtract(
        df_web_events
        .filter(~F.col("event_type").isin(["page_view", "click", "search"]))
        .select("page")
    )
)
