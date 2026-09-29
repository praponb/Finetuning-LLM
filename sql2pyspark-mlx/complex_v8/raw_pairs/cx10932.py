from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_status = Window.partitionBy(F.date_trunc("month", "start_date"), "status")
w_start_date_year_by_start_dat = Window.partitionBy(F.year("start_date")).orderBy(F.col("start_date").desc(), "id")
w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_status = Window.partitionBy("status")
w_next_start_date_month_by_pre = Window.partitionBy(F.month("next_start_date")).orderBy(F.col("premium_share_of_status").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_product_in_status_b = Window.partitionBy("distinct_product_in_status").orderBy(F.col("rn").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_product_in_status_b_2 = Window.partitionBy("distinct_product_in_status").orderBy(F.col("rn").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_distinct_product_in_status_b_3 = Window.partitionBy("distinct_product_in_status").orderBy(F.col("rn").desc_nulls_first())

recent_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") == "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)

metrics = (
    recent_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "product_clean",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_month_status), F.lit(0)), 4).alias("premium_share_of_start_date_month_status"),
        F.lead("premium").over(w_start_date_year_by_start_dat).alias("next_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_status_by_premium)).alias("days_since_prev_end_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_month_status), 2).alias("premium_vs_start_date_month_status_avg"),
        F.lead("start_date").over(w_start_date_year_by_start_dat).alias("next_start_date"),
        F.max("premium").over(w_status).alias("status_max_premium"),
        F.row_number().over(w_start_date_year_by_start_dat).alias("rn"),
        F.sum("premium").over(w_status).alias("status_sum_premium"),
        F.round(F.avg("premium").over(w_start_date_month_status), 2).alias("start_date_month_status_avg_premium"),
        F.count(F.lit(1)).over(w_start_date_month_status).alias("rows_in_start_date_month_status"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.nth_value("next_premium", 2).over(w_next_start_date_month_by_pre).alias("second_next_premium"),
        F.last("start_date", ignorenulls=True).over(w_distinct_product_in_status_b).alias("last_known_start_date"),
        F.last("next_premium").over(w_distinct_product_in_status_b_2).alias("last_next_premium"),
        F.sum("rn").over(w_rn).alias("rn_sum_rn"),
        F.round(F.avg("status_max_premium").over(w_rn), 2).alias("rn_avg_status_max_premium"),
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_rn), F.lit(0)), 4).alias("rn_share_of_rn"),
        F.round(F.cume_dist().over(w_distinct_product_in_status_b_3), 3).alias("rn_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "product_clean",
        "next_premium",
        "days_since_prev_end_date",
        "premium_share_of_status",
        "premium_vs_start_date_month_status_avg",
        "next_start_date",
        "status_max_premium",
        "rn",
        "status_sum_premium",
        "rows_in_start_date_month_status",
        "distinct_product_in_status",
        "second_next_premium",
        "last_known_start_date",
        "last_next_premium",
        "rn_sum_rn",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
    .orderBy(F.col("next_start_date").desc())
)
