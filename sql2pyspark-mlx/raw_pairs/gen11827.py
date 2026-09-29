from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("channel").orderBy("ends_on_quarter_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy("channel_count_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("starts_on")).orderBy("last_known_ends_on_quarter", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("channel") == "store") & F.col("channel").isNotNull())
    .select(
        "*",
        F.quarter("ends_on").alias("ends_on_quarter"),
        (F.datediff("starts_on", "ends_on") / 7).alias("weeks_ends_on_to_starts_on"),
    )
)

base_promotions = (
    base
    .filter(F.col("ends_on") > F.lit("2023-09-09").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "ends_on_quarter",
        "weeks_ends_on_to_starts_on",
        F.round("ends_on_quarter", 2).alias("ends_on_quarter_rounded"),
        (F.col("discount_pct") / F.nullif("weeks_ends_on_to_starts_on", F.lit(0))).alias("discount_pct_per_weeks_ends_on_to_starts_on"),
    )
)

metrics = (
    base_promotions
    .withColumn("channel_count_discount_pct", F.count("discount_pct").over(w))
    .withColumn("first_known_weeks_ends_on_to_starts_on", F.first("weeks_ends_on_to_starts_on", ignorenulls=True).over(w2))
)

scored = (
    metrics
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "discount_pct_per_weeks_ends_on_to_starts_on",
        "first_known_weeks_ends_on_to_starts_on",
        F.last("ends_on_quarter", ignorenulls=True).over(w_running).alias("last_known_ends_on_quarter"),
        F.sum("ends_on_quarter").over(w2).alias("cumulative_ends_on_quarter"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.avg("discount_pct_per_weeks_ends_on_to_starts_on").over(w4).alias("discount_pct_per_weeks_ends_on_to_starts_on_avg_6_prior"),
        F.last("channel", ignorenulls=True).over(w5).alias("last_known_channel"),
    )
)

result = (
    enriched
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list_2"),
    )
    .orderBy(F.col("channel").desc())
)
