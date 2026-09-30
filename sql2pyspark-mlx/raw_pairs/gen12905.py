from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("channel").orderBy("ends_on_quarter", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on_quarter").desc(), "id")
w4 = Window.partitionBy("channel").orderBy("ends_on_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy(F.col("ends_on_quarter_avg_5_prior").desc_nulls_first(), "id")
w6 = Window.partitionBy("promotion_promo_code")
w7 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")

valid = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-07-05").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.quarter("ends_on").alias("ends_on_quarter"))
)

metrics = (
    valid
    .withColumn("channel_max_ends_on_quarter", F.max("ends_on_quarter").over(w))
    .withColumn("ends_on_quarter_avg_5_prior", F.avg("ends_on_quarter").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("first_known_ends_on_quarter", F.first("ends_on_quarter", ignorenulls=True).over(w4))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

attributed = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics_promotions = (
    attributed
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "ends_on_quarter",
        "ends_on_quarter_avg_5_prior",
        "row_seq",
        "first_known_ends_on_quarter",
        "promo_id",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w6)), 4).alias("discount_pct_share"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    metrics_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "ends_on_quarter",
        "ends_on_quarter_avg_5_prior",
        "row_seq",
        "promo_id",
        "discount_pct_share",
    )
    .orderBy(F.col("ends_on_quarter").asc_nulls_first())
)
