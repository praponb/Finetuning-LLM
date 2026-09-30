from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on_month_end").asc_nulls_first())
w2 = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w3 = Window.partitionBy("promo_code").orderBy("ends_on_period", "id")
w4 = Window.partitionBy("promo_code").orderBy("ends_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("discount_pct") < 0.29)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_trunc("week", "ends_on").alias("ends_on_period"),
        F.last_day("ends_on").alias("ends_on_month_end"),
    )
)

latest = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_promotions = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

cleaned = (
    latest_promotions
    .filter(F.col("starts_on").between("2023-05-01", "2024-06-24"))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "ends_on_period",
        "ends_on_month_end",
        "row_num",
        "rn",
        F.length("promo_code").alias("promo_code_length"),
    )
)

with_prev = cleaned.withColumn("prev_ends_on_period", F.lag("ends_on_period").over(w3))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_ends_on_period").isNull() | (F.col("ends_on_period") > F.col("prev_ends_on_period") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w4))

result = (
    sessionized
    .groupBy(
        "promo_code",
        F.date_trunc("MONTH", "ends_on_month_end").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.avg(F.when(F.col("promo_code") == "VIP", F.col("discount_pct"))).alias("vip_avg_discount_pct"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count("*").alias("row_count"),
    )
    .orderBy("promo_code")
)
