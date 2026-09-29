from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month = Window.partitionBy("holder_id", F.month("end_date"))
w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")
w_start_date_year_by_start_dat = Window.partitionBy(F.year("start_date")).orderBy(F.col("start_date").asc_nulls_last(), "id")
w_start_date_year_by_start_dat_2 = Window.partitionBy(F.year("start_date")).orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_holder_id_by_holder_id_produ = Window.partitionBy("holder_id").orderBy("holder_id_product_sum_premium_distance", "id")
w_start_date_year_by_next2_pre = Window.partitionBy(F.year("start_date")).orderBy(F.col("next2_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_by_next2_pre_2 = Window.partitionBy(F.year("start_date")).orderBy(F.col("next2_premium").desc(), "id").rowsBetween(-3, -1)

recent_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2026-01-24").cast("date")) & (F.col("product").isin("HOME", "AUTO", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2033.8).alias("premium_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

sequenced = (
    recent_policys
    .select(
        "*",
        F.min("premium_distance").over(w_holder_id_end_date_month).alias("holder_id_end_date_month_min_premium_distance"),
        F.ntile(5).over(w_product_by_end_date).alias("end_date_quintile"),
        F.count(F.lit(1)).over(w_holder_id_end_date_month).alias("rows_in_holder_id_end_date_month"),
        F.lead("premium", 2).over(w_start_date_year_by_start_dat).alias("next2_premium"),
        F.sum("premium_distance").over(w_start_date_year_by_start_dat_2).alias("running_premium_distance"),
        F.min("premium_distance").over(w_holder_id).alias("holder_id_min_premium_distance"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
        F.sum("premium_distance").over(w_holder_id_product).alias("holder_id_product_sum_premium_distance"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_by_holder_id_produ), 3).alias("holder_id_product_sum_premium_distance_cume_dist"),
        F.lead("start_date").over(w_holder_id_by_holder_id_produ).alias("next_start_date"),
        F.round(F.col("next2_premium") - F.avg("next2_premium").over(w_holder_id), 2).alias("next2_premium_vs_holder_id_avg"),
        F.nth_value("running_premium_distance", 2).over(w_start_date_year_by_next2_pre).alias("second_running_premium_distance"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_holder_id), F.lit(0)), 4).alias("premium_distance_share_of_holder_id"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_holder_id_produ)).alias("days_since_prev_end_date"),
        F.round(F.avg("end_date_quintile").over(w_start_date_year_by_next2_pre_2), 2).alias("end_date_quintile_avg_3_prior"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "holder_id_end_date_month_min_premium_distance",
        "end_date_quintile",
        "rows_in_holder_id_end_date_month",
        "running_premium_distance",
        "holder_id_product_sum_premium_distance_cume_dist",
        "next2_premium_vs_holder_id_avg",
        "second_running_premium_distance",
        "premium_distance_share_of_holder_id",
        "days_since_prev_end_date",
        "end_date_quintile_avg_3_prior",
    )
    .orderBy(F.col("running_premium_distance").desc())
)
