from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_premium_tier_holder_id_by_en = Window.partitionBy("premium_tier", "holder_id").orderBy("end_date", "id")
w_premium_tier_holder_id_by_en_2 = Window.partitionBy("premium_tier", "holder_id").orderBy("end_date", "id").rowsBetween(-1, Window.currentRow)
w_premium_tier_holder_id_by_en_3 = Window.partitionBy("premium_tier", "holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id")
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_premium_tier = Window.partitionBy("premium_tier")
w_all_by_first_start_date_movi = Window.orderBy(F.col("first_start_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("next_premium").desc())
w_holder_id_end_date_month_by_2 = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("next_premium").desc(), "id")

filtered_policys = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2026-09-19").cast("date"))) & (F.col("product").isin("HEALTH", "LIFE", "AUTO")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.when(F.col("premium") >= 3465.4, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

windowed = (
    filtered_policys
    .select(
        "*",
        F.size(F.collect_set("premium_tier").over(w_status)).alias("distinct_premium_tier_in_status"),
        F.ntile(3).over(w_premium_tier_holder_id_by_en).alias("end_date_tercile"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.round(F.avg("premium").over(w_premium_tier_holder_id_by_en_2), 2).alias("premium_avg_2"),
        F.nth_value("premium", 2).over(w_premium_tier_holder_id_by_en_3).alias("second_premium"),
        F.first("start_date").over(w_all_by_end_date_full).alias("first_start_date"),
        F.lead("premium").over(w_status_holder_id_by_start_da).alias("next_premium"),
        F.nth_value("start_date", 2).over(w_status_holder_id_by_start_da_2).alias("second_start_date"),
        F.row_number().over(w_all_by_end_date).alias("seq_no"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.round(F.col("second_premium") - F.avg("second_premium").over(w_premium_tier), 2).alias("second_premium_vs_premium_tier_avg"),
        F.round(F.avg("premium_share_of_status").over(w_all_by_first_start_date_movi), 2).alias("premium_share_of_status_avg_4"),
        F.round(F.cume_dist().over(w_holder_id_end_date_month_by), 3).alias("next_premium_cume_dist"),
        F.round(F.cume_dist().over(w_holder_id_end_date_month_by_2), 3).alias("next_premium_cume_dist_2"),
        F.round(F.col("premium_share_of_status") - F.avg("premium_share_of_status").over(w_premium_tier), 2).alias("premium_share_of_status_vs_premium_tier_avg"),
        F.min("next_premium").over(w_premium_tier).alias("premium_tier_min_next_premium"),
        F.round(F.col("premium_avg_2") - F.avg("premium_avg_2").over(w_premium_tier), 2).alias("premium_avg_2_vs_premium_tier_avg"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_slug",
        "distinct_premium_tier_in_status",
        "premium_avg_2",
        "second_premium",
        "first_start_date",
        "next_premium",
        "next_premium_cume_dist_2",
        "premium_share_of_status_vs_premium_tier_avg",
        (F.col("premium_avg_2") + F.col("next_premium")).alias("premium_avg_2_plus_next_premium"),
    )
    .orderBy("next_premium", F.col("first_start_date").desc())
)
