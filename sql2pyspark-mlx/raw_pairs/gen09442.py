from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id")
w3 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("ends_on", "id")
w_part = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "channel", "ends_on")
)

enriched = (
    deduped
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

scored = (
    enriched
    .select(
        "id",
        "last_known_channel",
        F.min("seq_no").over(w_part).alias("last7_min_seq_no"),
        F.avg("seq_no").over(w_part).alias("last7_avg_seq_no"),
        F.max("seq_no").over(w_part).alias("last7_max_seq_no"),
        F.count("seq_no").over(w_part).alias("last7_count_seq_no"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last7_max_seq_no") >= 7.4, "PREMIUM").otherwise("STANDARD").alias("last7_max_seq_no_tier"),
        "last7_count_seq_no",
    )
    .agg(
        F.avg("last7_count_seq_no").alias("avg_last7_count_seq_no"),
        F.count("*").alias("row_count"),
        F.percentile_approx("last7_count_seq_no", [0.25, 0.5, 0.75]).alias("last7_count_seq_no_quartiles"),
    )
)
