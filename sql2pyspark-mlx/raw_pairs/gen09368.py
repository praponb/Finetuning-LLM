from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("page").orderBy("duration_sec", "id")
w5 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w6 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rank").desc(), "id")
w7 = Window.partitionBy("browser").orderBy("duration_sec_cume_dist", "id")
w8 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_avg_5_prior").desc(), "id")
w9 = Window.partitionBy("event_type").orderBy("rank_in_group", "id")
w10 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_first())
w11 = Window.orderBy("prev_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/product"))) & (F.col("event_time") >= F.lit("2023-03-15").cast("date")))
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 424, "High").when(F.col("duration_sec") >= 200, "Medium").otherwise("Low"))
)

enriched = (
    valid
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "duration_sec_band",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_5_prior"),
        F.lag("duration_sec", 1).over(w4).alias("prev_duration_sec"),
        F.rank().over(w5).alias("duration_sec_rank"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    deduped
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.country") == "TH")
    .select("a.*", F.col("d.browser"))
)

metrics = (
    with_dims
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("duration_sec_avg_5_prior_bucket", F.ntile(3).over(w8))
    .withColumn("prev_duration_sec_bucket", F.lag("duration_sec_bucket", 1).over(w9))
    .withColumn("duration_sec_rank_2", F.rank().over(w10))
    .withColumn("cumulative_duration_sec_bucket", F.sum("duration_sec_bucket").over(w11))
)

by_type = (
    metrics
    .groupBy("browser")
    .pivot("event_type", ["click", "add_to_cart", "search"])
    .agg(F.max("duration_sec_rank_2"))
)

result = (
    by_type
    .select(
        "browser",
        "click",
        "add_to_cart",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "GROUP_A").when(F.col("browser").rlike("^(Safari|Firefox)$"), "OTHER_ZONE").otherwise("CORE").alias("browser_zone"),
    )
    .orderBy(F.col("click").desc_nulls_first())
)
