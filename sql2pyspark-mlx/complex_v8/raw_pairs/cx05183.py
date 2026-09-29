from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month = Window.partitionBy(F.month("start_date"))
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_all_by_premium_full = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_policies
    .filter(F.col("product").isin("TRAVEL", "HEALTH", "LIFE"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

enriched = (
    base
    .filter(~F.col("status").isin("EXPIRED", "LAPSED"))
    .select(
        "*",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_month), F.lit(0)), 4).alias("premium_share_of_start_date_month"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_premium)).alias("days_since_prev_start_date"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        F.nth_value("premium", 2).over(w_all_by_premium_full).alias("second_premium"),
        F.size(F.collect_set("status").over(w_start_date_month)).alias("distinct_status_in_start_date_month"),
        F.round(F.avg("premium").over(w_start_date_month), 2).alias("start_date_month_avg_premium"),
    )
)

labeled = (
    sequenced
    .filter(F.col("start_date") >= F.lit("2024-12-03").cast("date"))
    .select(
        "*",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        F.when(F.col("second_premium") >= 3544.1, "A").when(F.col("second_premium") >= 2013.8, "B").otherwise("C").alias("second_premium_tier"),
    )
)

result = (
    labeled
    .groupingSets(
        [["distinct_status_in_start_date_month", "holder_id", "second_premium_tier"], ["distinct_status_in_start_date_month"]],
        "distinct_status_in_start_date_month", "holder_id", "second_premium_tier",
    )
    .agg(
        F.grouping("distinct_status_in_start_date_month").alias("is_total_distinct_status_in_start_date_month"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .orderBy(
        F.col("distinct_status_in_start_date_month").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("second_premium_tier").asc_nulls_last(),
    )
)
