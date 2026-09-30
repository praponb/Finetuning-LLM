from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_prio = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(-3, -1)
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_premium_tier = Window.partitionBy("holder_id", "premium_tier")
w_status = Window.partitionBy("status")
w_holder_id_premium_tier_by_en = Window.partitionBy("holder_id", "premium_tier").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id")

scoped_policys = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.col("premium") < 3719.7))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3719.7, "large").when(F.col("premium") >= 1541.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

with_windows = (
    scoped_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_tier",
        F.sum("premium").over(w_holder_id_by_start_date_prio).alias("premium_sum_3_prior"),
        F.sum("premium").over(w_holder_id_by_end_date_runnin).alias("running_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_premium_tier), F.lit(0)), 4).alias("premium_share_of_holder_id_premium_tier"),
        F.sum("premium").over(w_holder_id_premium_tier).alias("holder_id_premium_tier_sum_premium"),
        F.max("premium").over(w_holder_id_premium_tier).alias("holder_id_premium_tier_max_premium"),
        F.min("premium").over(w_status).alias("status_min_premium"),
        F.nth_value("end_date", 2).over(w_holder_id_premium_tier_by_en).alias("second_end_date"),
        F.size(F.collect_set("status").over(w_holder_id_premium_tier)).alias("distinct_status_in_holder_id_premium_tier"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_premium_tier), 2).alias("premium_vs_holder_id_premium_tier_avg"),
        F.first("premium").over(w_holder_id_by_start_date_full).alias("first_premium"),
        F.row_number().over(w_holder_id_by_end_date).alias("rn"),
        F.ntile(10).over(w_holder_id_by_end_date).alias("end_date_decile"),
    )
)

top_rows = with_windows.filter(F.col("rn") <= 5)

result = (
    top_rows
    .filter(F.col("start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-07-07").cast("date")))
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_tier",
        "premium_sum_3_prior",
        "running_premium",
        "premium_share_of_holder_id_premium_tier",
        "holder_id_premium_tier_sum_premium",
        "holder_id_premium_tier_max_premium",
        "status_min_premium",
        "second_end_date",
        "distinct_status_in_holder_id_premium_tier",
        "premium_vs_holder_id_premium_tier_avg",
        "first_premium",
        "rn",
        "end_date_decile",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.when(F.col("premium_tier").isin("small", "medium"), "group_a").when(F.col("premium_tier") == "large", "group_b").otherwise("other").alias("premium_tier_group"),
    )
    .orderBy(
        F.col("premium_vs_holder_id_premium_tier_avg").desc(),
        F.col("status").desc_nulls_first(),
    )
)
