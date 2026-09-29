from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 259) & (F.to_date("event_time") < "2024-04-09"))
    .select(
        "page",
        "event_type",
        F.when(F.col("event_type").isin(["page_view", "add_to_cart"]), "Group A").otherwise("Other").alias("event_type_group"),
        F.when(F.col("duration_sec") >= 411, "High").when(F.col("duration_sec") >= 170, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)
