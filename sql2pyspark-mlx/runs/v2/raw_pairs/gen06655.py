from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & (F.col("duration_sec") < 314))
    .select(
        "id",
        "page",
        F.when(F.col("page").isin(["/cart", "/product"]), "Tier 1").otherwise("Other").alias("page_group"),
        F.when(F.col("duration_sec") >= 527, "High").when(F.col("duration_sec") >= 84, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)
