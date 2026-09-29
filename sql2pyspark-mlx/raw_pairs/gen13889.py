from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-07-26"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.when(F.col("channel").isin(["mobile", "partner"]), "Tier 1").otherwise("Other").alias("channel_group"),
        F.trim("promo_code").alias("clean_promo_code"),
    )
)

filtered = (
    valid
    .filter((F.col("starts_on") >= F.lit("2023-07-13").cast("date")) & (F.col("starts_on") >= "2024-10-01"))
    .withColumn("starts_on_label", F.date_format("starts_on", "yyyy-MM"))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    latest
    .filter(F.col("starts_on") > F.lit("2023-08-27").cast("date"))
    .select("*", F.date_trunc("year", "starts_on").alias("starts_on_period"))
)

report = (
    base
    .select(
        "id",
        "starts_on",
        "ends_on",
        "channel_group",
        "clean_promo_code",
        "starts_on_label",
        "starts_on_period",
    )
    .orderBy(F.col("starts_on_period").asc_nulls_first(), F.col("ends_on").desc())
)

result = (
    report
    .select("id", "ends_on", "channel_group", "clean_promo_code", "starts_on_period")
)
