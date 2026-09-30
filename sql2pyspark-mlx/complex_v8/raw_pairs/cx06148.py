from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_end_date = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
w_all_by_end_date_running = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_product_by_end_date_running = Window.partitionBy("product").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_status_by_end_date_moving_3 = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")
w_status = Window.partitionBy("status")
w_end_date_month_status_by_pre = Window.partitionBy(F.month("end_date"), "status").orderBy(F.col("prev3_days_since_end_date").asc_nulls_last())
w_product_holder_id_by_min_day = Window.partitionBy("product", "holder_id").orderBy("min_days_since_end_date_so_far", "id")
w_holder_id_by_last_known_days = Window.partitionBy("holder_id").orderBy("last_known_days_since_end_date", "id").rowsBetween(-6, Window.currentRow)
w_end_date_month_status_by_pre_2 = Window.partitionBy(F.month("end_date"), "status").orderBy(F.col("prev3_days_since_end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_last_known_days_2 = Window.partitionBy("holder_id").orderBy("last_known_days_since_end_date", "id")
w_holder_id_by_days_since_end = Window.partitionBy("holder_id").orderBy("days_since_end_date", "id")
w_product = Window.partitionBy("product")

base_policys = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

ranked = (
    base_policys
    .select(
        "*",
        F.lag("days_since_end_date").over(w_status_by_end_date).alias("prev_days_since_end_date"),
        F.min("days_since_end_date").over(w_all_by_end_date_running).alias("min_days_since_end_date_so_far"),
        F.round(F.col("days_since_end_date") / F.nullif(F.sum("days_since_end_date").over(w_product_holder_id), F.lit(0)), 4).alias("days_since_end_date_share_of_product_holder_id"),
        F.last("days_since_end_date", ignorenulls=True).over(w_product_by_end_date_running).alias("last_known_days_since_end_date"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_end_date)).alias("days_since_prev_end_date"),
        F.sum("days_since_end_date").over(w_status_by_end_date_moving_3).alias("days_since_end_date_sum_4"),
        F.lag("days_since_end_date", 3, 0).over(w_product_by_end_date).alias("prev3_days_since_end_date"),
        F.round(F.percent_rank().over(w_all_by_end_date), 3).alias("end_date_percent_rank"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "product",
        "end_date",
        "days_since_end_date_share_of_product_holder_id",
        "last_known_days_since_end_date",
        "days_since_prev_end_date",
        "days_since_end_date_sum_4",
        "end_date_percent_rank",
        F.max("prev3_days_since_end_date").over(w_status).alias("status_max_prev3_days_since_end_date"),
        F.round(F.percent_rank().over(w_end_date_month_status_by_pre), 3).alias("prev3_days_since_end_date_percent_rank"),
        F.lead("days_since_end_date", 3).over(w_product_holder_id_by_min_day).alias("next3_days_since_end_date"),
        F.sum("days_since_end_date").over(w_holder_id_by_last_known_days).alias("days_since_end_date_sum_7"),
        F.last("end_date").over(w_end_date_month_status_by_pre_2).alias("last_end_date"),
        F.lead("prev_days_since_end_date", 3, 0).over(w_holder_id_by_last_known_days_2).alias("next3_prev_days_since_end_date"),
        F.ntile(4).over(w_holder_id_by_days_since_end).alias("days_since_end_date_quartile"),
        F.round(F.col("days_since_end_date_sum_4") - F.avg("days_since_end_date_sum_4").over(w_product), 2).alias("days_since_end_date_sum_4_vs_product_avg"),
    )
)

result = (
    with_windows
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "days_since_end_date_share_of_product_holder_id",
        "last_known_days_since_end_date",
        "days_since_prev_end_date",
        "end_date_percent_rank",
        "status_max_prev3_days_since_end_date",
        "prev3_days_since_end_date_percent_rank",
        "next3_days_since_end_date",
        "days_since_end_date_sum_7",
        "last_end_date",
        "days_since_end_date_quartile",
        "days_since_end_date_sum_4_vs_product_avg",
        F.when(F.col("end_date_percent_rank") >= 0.8, "large").when(F.col("end_date_percent_rank") >= 0.3, "medium").otherwise("small").alias("end_date_percent_rank_tier"),
        F.round(F.col("days_since_prev_end_date") / F.nullif(F.col("end_date_percent_rank"), F.lit(0)), 3).alias("days_since_prev_end_date_per_end_date_percent_ra"),
    )
    .orderBy("id", "days_since_end_date_sum_4_vs_product_avg")
    .limit(5)
)
