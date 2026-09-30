from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy(F.month("ends_on"), "channel")
w_grp = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-08-01").cast("date"))
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 6))
)

scored = (
    filtered
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.rank().over(w).alias("discount_pct_rank"),
        F.ntile(3).over(w2).alias("discount_pct_bucket"),
        F.stddev_samp("discount_pct").over(w3).alias("promo_code_stddev_samp_discount_pct"),
        F.count("discount_pct").over(w4).alias("ends_on_month_count_discount_pct"),
        F.first("channel", ignorenulls=True).over(w_grp).alias("first_known_channel"),
    )
)

filtered_promotions = (
    scored
    .filter(F.col("discount_pct_rank").between(3, 5))
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "discount_pct_bucket",
        "promo_code_stddev_samp_discount_pct",
        "ends_on_month_count_discount_pct",
        "first_known_channel",
        F.greatest("discount_pct_rank", "promo_code_stddev_samp_discount_pct").alias("greatest_discount_pct_rank_promo_code_stddev_samp_discount_pct"),
    )
)

filtered_promotions_2 = (
    filtered_promotions
    .filter(F.col("greatest_discount_pct_rank_promo_code_stddev_samp_discount_pct") <= 32)
    .withColumn("discount_pct_whole", F.ceil("discount_pct"))
)

base = (
    filtered_promotions_2
    .filter(F.col("ends_on_month_count_discount_pct") <= 13)
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 1))
)

result = (
    base
    .groupBy(F.date_trunc("month", "starts_on_plus_months").alias("activity_month"))
    .agg(
        F.percentile_approx("discount_pct_whole", 0.9).alias("discount_pct_whole_p90"),
        F.sum("discount_pct_whole").alias("total_discount_pct_whole"),
        F.count("*").alias("event_count"),
    )
    .orderBy("discount_pct_whole_p90")
)
