from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.orderBy("rank_in_group", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("promo_code", "channel").orderBy(F.col("rank_in_group").desc(), "id")
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

latest = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    latest
    .filter((F.col("starts_on") > F.lit("2023-04-12").cast("date")) & (F.col("promo_code") == "VIP"))
    .select("*", F.upper("channel").alias("channel_upper"))
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_3_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.cume_dist().over(w4).alias("discount_pct_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("discount_pct_avg_3_prior") != 0)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "rank_in_group",
        "channel_upper",
        "discount_pct_avg_3_prior",
        "seq_no",
        "discount_pct_cume_dist",
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "rank_in_group",
        "discount_pct_avg_3_prior",
        "discount_pct_cume_dist",
        "days_since_ends_on",
        "resolved_channel",
    )
    .orderBy(F.col("starts_on").desc_nulls_first())
)
