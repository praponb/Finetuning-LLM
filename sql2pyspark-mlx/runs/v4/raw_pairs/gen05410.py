from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w_grp = Window.partitionBy("session_id", "event_type").orderBy("event_time_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w6 = Window.partitionBy("event_type", "session_id").orderBy(F.col("new_group_flag").asc_nulls_first(), "id")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2024-03-13"))) & F.col("page").isNotNull() & ~F.col("event_type").isin(["click", "search", "add_to_cart"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("event_time") >= F.lit("2024-03-02").cast("date"))
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["search", "page_view"]), "Tier 1").otherwise("Other"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
)

gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

deduped = (
    sessionized
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.avg("new_group_flag").over(w_grp).alias("new_group_flag_avg_6_prior"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

deduped_web_events = (
    with_windows
    .withColumn("rn_2", F.row_number().over(w6))
    .filter(F.col("rn_2") == 1)
)

stats = (
    deduped_web_events
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_rows"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("click_rows") > 1)
)

result = (
    stats
    .select("new_group_flag_tier", "add_to_cart_rows", "click_rows", "event_type_list")
    .orderBy(F.col("new_group_flag_tier").desc())
)
