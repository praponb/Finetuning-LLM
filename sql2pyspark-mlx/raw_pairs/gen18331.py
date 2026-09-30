from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("page_event_type", F.concat("page", F.lit("/"), "event_type"))
)

enriched = (
    filtered
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Edge", "Chrome"]))
    .select(
        "a.id",
        "a.session_id",
        "a.event_time",
        F.col("s.country"),
        F.col("s.device"),
        F.col("s.browser"),
    )
)

enriched_web_events = (
    enriched
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_ordered))
)

valid = (
    enriched_web_events
    .filter(F.col("event_time") > F.lit("2023-10-06").cast("date"))
    .withColumn("is_uk", F.when(F.col("country") == "UK", 1).otherwise(0))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM-dd"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("NONE")))
)

filtered_web_events = (
    valid
    .filter((F.col("event_time") >= F.lit("2023-09-01").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "event_time",
        "country",
        "device",
        "browser",
        "last_known_country",
        "is_uk",
        "browser_clean",
        F.round((F.col("is_uk") * 0.9), 2).alias("is_uk_scaled"),
        F.coalesce("device", F.lit("DIRECT")).alias("resolved_device"),
    )
)

result = (
    filtered_web_events
    .groupBy("device")
    .agg(
        F.sum(F.when(F.col("browser") == "Edge", F.col("is_uk_scaled")).otherwise(0)).alias("edge_is_uk_scaled"),
        F.percentile_approx("is_uk_scaled", [0.25, 0.5, 0.75]).alias("is_uk_scaled_quartiles"),
        F.round(F.sum("is_uk_scaled") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("is_uk_scaled_per_mobile"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.avg(F.when(F.col("device") == "mobile", F.col("is_uk"))).alias("mobile_avg_is_uk"),
        F.sum("is_uk").alias("total_is_uk"),
    )
    .filter(F.col("mobile_avg_is_uk") < 1000000)
    .orderBy(F.col("mobile_avg_is_uk").desc_nulls_last())
)
