from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("is_welcome5").asc_nulls_last(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")

cleaned = (
    df_promotions
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-10-09")))
    .select(
        "*",
        F.when(F.col("promo_code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

valid = (
    cleaned
    .filter(F.col("ends_on") <= "2023-12-01")
    .select(
        "*",
        F.round((F.datediff("ends_on", "starts_on") / 30), 1).alias("months_starts_on_to_ends_on"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid_promotions = (
    deduped
    .filter((F.col("ends_on") >= F.lit("2023-07-29").cast("date")) & (F.col("months_starts_on_to_ends_on") <= 27))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "is_welcome5",
        "months_starts_on_to_ends_on",
        "latest_rank",
        F.date_trunc("YEAR", "ends_on").alias("ends_on_period"),
    )
)

latest = (
    valid_promotions
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

daily = (
    latest
    .withColumn("is_welcome5_tier", F.when(F.col("is_welcome5") >= 72, "LARGE").otherwise("SMALL"))
    .withColumn("months_starts_on_to_ends_on_tier", F.when(F.col("months_starts_on_to_ends_on") >= 84, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .groupingSets(
        [["is_welcome5_tier", "months_starts_on_to_ends_on_tier", "activity_month"], ["is_welcome5_tier", "months_starts_on_to_ends_on_tier"]],
        "is_welcome5_tier",
        "months_starts_on_to_ends_on_tier",
        "activity_month",
    )
    .agg(
        F.max("ends_on_period").alias("last_ends_on_period"),
        F.max(F.when(F.col("promo_code") == "SPRING10", F.col("is_welcome5"))).alias("spring10_max_is_welcome5"),
        F.min("ends_on_period").alias("first_seen"),
        F.grouping("is_welcome5_tier").alias("grouping_level"),
    )
)

result = (
    daily
    .select("spring10_max_is_welcome5", "grouping_level")
    .orderBy("grouping_level")
)
