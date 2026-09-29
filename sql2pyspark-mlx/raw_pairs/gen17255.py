from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.when(F.col("discount_pct") >= 0.3, "High").when(F.col("discount_pct") >= 0.13, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    ranked
    .select("id", "channel")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") > 0.21)
        .select("id", "channel")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("starts_on") >= F.lit("2023-09-20").cast("date"))
    .withColumn("channel_upper", F.upper("channel"))
)

result = (
    filtered
    .withColumn("activity_month", F.date_trunc("month", "ends_on"))
    .rollup("activity_month", "promo_code")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("channel")).alias("num_channels_2"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.grouping("activity_month").alias("gid"),
    )
    .filter(F.col("spring10_events") < 1000)
)
