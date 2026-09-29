from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("row_num", "id")
w6 = Window.orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("running_discount_pct_whole", "id")
w8 = Window.orderBy("running_discount_pct_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("discount_pct_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("last3_max_last_known_discount_pct_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-14"))) & (F.col("ends_on") > "2023-06-01"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .filter(F.col("discount_pct_whole").isNotNull())
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_whole",
        "row_num",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.count("discount_pct_whole").over(w2).alias("promo_code_count_discount_pct_whole"),
        F.sum("discount_pct_whole").over(w3).alias("running_discount_pct_whole"),
        F.last("discount_pct_whole", ignorenulls=True).over(w4).alias("last_known_discount_pct_whole"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

enriched = (
    scored
    .withColumn("last3_max_last_known_discount_pct_whole", F.max("last_known_discount_pct_whole").over(w6))
    .withColumn("last3_avg_last_known_discount_pct_whole", F.avg("last_known_discount_pct_whole").over(w6))
    .withColumn("last3_count_last_known_discount_pct_whole", F.count("last_known_discount_pct_whole").over(w6))
    .withColumn("running_discount_pct_whole_bucket", F.ntile(10).over(w7))
)

with_windows = (
    enriched
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "row_num",
        "discount_pct_share",
        "promo_code_count_discount_pct_whole",
        "running_discount_pct_whole",
        "last_known_discount_pct_whole",
        "last3_max_last_known_discount_pct_whole",
        "last3_count_last_known_discount_pct_whole",
        "running_discount_pct_whole_bucket",
        F.sum("discount_pct_whole").over(w8).alias("cumulative_discount_pct_whole"),
        F.first("last3_avg_last_known_discount_pct_whole", ignorenulls=True).over(w9).alias("first_known_last3_avg_last_known_discount_pct_whole"),
        F.sum("last3_count_last_known_discount_pct_whole").over(w10).alias("cumulative_last3_count_last_known_discount_pct_whole"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.min("last3_count_last_known_discount_pct_whole").alias("min_last3_count_last_known_discount_pct_whole"),
        F.percentile_approx("discount_pct_share", [0.25, 0.5, 0.75]).alias("discount_pct_share_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
