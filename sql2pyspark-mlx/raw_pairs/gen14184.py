from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on")
w2 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w_ordered = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct_percent_rank", "id")

latest = (
    df_promotions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .filter(F.col("rn") > 1)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
        F.percent_rank().over(w_grp).alias("discount_pct_percent_rank"),
    )
)

latest_promotions = (
    metrics
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest_promotions
    .select(
        "*",
        F.lag("discount_pct", 1).over(w_ordered).alias("prev_discount_pct"),
        F.ntile(10).over(w_ordered).alias("discount_pct_bucket"),
        F.lag("discount_pct", 2).over(w6).alias("prev_discount_pct_2"),
    )
)

filtered = (
    with_windows
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-08-19")))
    .withColumn("clean_promo_code", F.trim("promo_code"))
)

result = (
    filtered
    .groupBy(F.date_trunc("QUARTER", "ends_on").alias("quarter_start"))
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.round(F.avg("discount_pct_percent_rank"), 2).alias("avg_discount_pct_percent_rank"),
    )
    .filter(F.col("avg_discount_pct_percent_rank") > 0)
)
