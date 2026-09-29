from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_group_by_premium_buck = Window.partitionBy("status_group").orderBy(F.col("premium_bucket_product_home").desc_nulls_first(), "end_date_month", "status_group").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_group_by_premium_buck_2 = Window.partitionBy("status_group").orderBy(F.col("premium_bucket_product_home").desc_nulls_first(), "end_date_month", "status_group")
w_status_group_by_premium_buck_3 = Window.partitionBy("status_group").orderBy(F.col("premium_bucket_product_home").desc_nulls_first())
w_last_end_date_year_distinct = Window.partitionBy(F.year("last_end_date"), "distinct_status")

filtered = (
    df_policies
    .filter((F.col("premium") <= 3544.1) & (F.col("holder_id") >= 18))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.when(F.col("status").isin("CANCELLED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("premium") >= 3465.4, "large").when(F.col("premium") >= 2111.3, "medium").otherwise("small").alias("premium_tier"),
    )
)

per_group = (
    filtered
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "status_group",
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("product") == "HOME", F.col("premium_bucket"))).alias("premium_bucket_product_home"),
        F.max("end_date").alias("last_end_date"),
        F.min("premium").alias("min_premium"),
        F.min("end_date").alias("first_end_date"),
        F.avg("premium").alias("avg_premium_check"),
    )
    .filter(F.col("avg_premium_check") > 1541.8)
    .drop("avg_premium_check")
)

screened = (
    per_group
    .crossJoin(per_group.agg(F.min("distinct_status").alias("overall_min_distinct_status")))
    .filter(F.col("distinct_status") > F.col("overall_min_distinct_status"))
    .drop("overall_min_distinct_status")
)

ranked = (
    screened
    .select(
        "*",
        F.nth_value("premium_bucket_product_home", 2).over(w_status_group_by_premium_buck).alias("second_premium_bucket_product_home"),
        F.lag("first_end_date").over(w_status_group_by_premium_buck_2).alias("prev_first_end_date"),
        F.round(F.cume_dist().over(w_status_group_by_premium_buck_3), 3).alias("premium_bucket_product_home_cume_dist"),
        F.min("distinct_status").over(w_last_end_date_year_distinct).alias("last_end_date_year_distinct_status_min_distinct_"),
    )
)

without_matches = (
    ranked
    .crossJoin(ranked.agg(F.avg("distinct_status").alias("overall_avg_distinct_status")))
    .filter(F.col("distinct_status") > F.col("overall_avg_distinct_status"))
    .drop("overall_avg_distinct_status")
)

result = (
    without_matches
    .select(
        "end_date_month",
        "status_group",
        "distinct_status",
        "premium_bucket_product_home",
        "min_premium",
        "first_end_date",
        F.round(F.col("distinct_status") / F.nullif(F.col("min_premium"), F.lit(0)), 3).alias("distinct_status_per_min_premium"),
    )
    .orderBy(
        "min_premium",
        F.col("first_end_date").desc(),
        "end_date_month",
        "status_group",
    )
    .limit(20)
)
