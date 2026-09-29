from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "click", "add_to_cart"]))
    .select("id", "duration_sec", F.coalesce("page", F.lit("N/A")).alias("page_filled"))
)
