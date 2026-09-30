from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("row_num", "id").rowsBetween(-3, -1)
w_hist = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2024-01-04").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

valid = (
    base
    .filter((F.col("ends_on") > F.to_date(F.lit("2024-05-19"))) & (F.col("discount_pct") > 0.14))
    .withColumn("channel_lower", F.lower("channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

latest = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.lag("discount_pct", 1).over(w2).alias("prev_discount_pct"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_3_prior"),
        F.last("channel", ignorenulls=True).over(w_hist).alias("last_known_channel"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        F.col("discount_pct").alias("DISCOUNT_PCT_VAL"),
        F.col("prev_discount_pct").alias("PREV_DISCOUNT_PCT"),
    )
    .unpivot(
        ["id", "channel"],
        ["DISCOUNT_PCT_VAL", "PREV_DISCOUNT_PCT"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
