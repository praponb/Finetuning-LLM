from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc())
w3 = Window.partitionBy("channel").orderBy("ends_on_minus_81d", "id")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & ~F.col("channel").isin(["store", "mobile"]))
    .select(
        "*",
        F.date_sub("ends_on", 81).alias("ends_on_minus_81d"),
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

latest_promotions = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w3))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

result = (
    latest_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "ends_on_minus_81d",
        "ends_on_month_end",
        "channel_filled",
        "row_num",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "EXTENDED").when(F.col("channel").rlike("^(web|store)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("ends_on_minus_81d").desc())
)
