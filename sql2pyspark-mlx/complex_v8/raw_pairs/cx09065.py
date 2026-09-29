from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_status_by_en = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("end_date").desc_nulls_first(), "id")
w_start_date_year_status_by_en_2 = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("end_date").desc_nulls_first())
w_all_by_max_policy_premium_mo = Window.orderBy(F.col("max_policy_premium").desc(), "id").rowsBetween(-3, Window.currentRow)
w_start_date_month_status = Window.partitionBy(F.date_trunc("month", "start_date"), "status")

recent = (
    df_policies
    .filter((~F.col("status").isin("ACTIVE", "EXPIRED")) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_policy_premium"))
)

with_counts = (
    recent.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.max_policy_premium")
)

with_windows = (
    with_counts
    .select(
        "*",
        F.lag("premium").over(w_start_date_year_status_by_en).alias("prev_premium"),
        F.round(F.cume_dist().over(w_start_date_year_status_by_en_2), 3).alias("end_date_cume_dist"),
        F.sum("premium").over(w_all_by_max_policy_premium_mo).alias("premium_sum_4"),
        F.round(F.col("max_policy_premium") / F.nullif(F.sum("max_policy_premium").over(w_start_date_month_status), F.lit(0)), 4).alias("max_policy_premium_share_of_start_date_month_sta"),
        F.min("max_policy_premium").over(w_start_date_month_status).alias("start_date_month_status_min_max_policy_premium"),
        F.count(F.lit(1)).over(w_start_date_month_status).alias("rows_in_start_date_month_status"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.year(F.col("start_date")).alias("start_date_year"),
    )
)

result = (
    tagged
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "prev_premium",
        "end_date_cume_dist",
        "premium_sum_4",
        "max_policy_premium_share_of_start_date_month_sta",
        "rows_in_start_date_month_status",
        "start_date_year",
        F.when(F.col("premium_sum_4") >= 11300.0, "A").when(F.col("premium_sum_4") >= 6120.0, "B").otherwise("C").alias("premium_sum_4_tier"),
    )
    .orderBy(F.col("holder_id").desc(), F.col("end_date_cume_dist").asc())
)
