from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_by_holder_jo = Window.partitionBy(F.year("start_date")).orderBy("holder_joined_on", "id")
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_product_group_status_by_prem = Window.partitionBy("product_group", "status").orderBy("premium")
w_product_group_status_by_prem_2 = Window.partitionBy("product_group", "status").orderBy("premium", "id").rowsBetween(-3, Window.currentRow)
w_product_group_status_by_prem_3 = Window.partitionBy("product_group", "status").orderBy("premium", "id")
w_holder_id_product_group_by_h = Window.partitionBy("holder_id", "product_group").orderBy("holder_joined_on")
w_holder_joined_on_decile_hold = Window.partitionBy("holder_joined_on_decile", F.to_date("holder_joined_on_month")).orderBy(F.col("total_premium_dense_rank").desc())
w_holder_joined_on_decile_hold_2 = Window.partitionBy("holder_joined_on_decile", F.to_date("holder_joined_on_month")).orderBy(F.col("total_premium_dense_rank").desc(), "start_date_ym", "holder_joined_on_month", "holder_joined_on_decile").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_premium_dense_r = Window.orderBy(F.col("total_premium_dense_rank").desc(), "start_date_ym", "holder_joined_on_month", "holder_joined_on_decile")

valid = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-09-23").cast("date")) & (F.col("premium") > 1042.8))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("LIFE", "HEALTH"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
    )
)

joined = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

ranked = (
    joined
    .select(
        "*",
        F.ntile(10).over(w_start_date_year_by_holder_jo).alias("holder_joined_on_decile"),
        F.round(F.avg("premium").over(w_end_date_month), 2).alias("end_date_month_avg_premium"),
        F.dense_rank().over(w_product_group_status_by_prem).alias("premium_dense_rank"),
        F.sum("premium").over(w_product_group_status_by_prem_2).alias("premium_sum_4"),
        F.sum("premium").over(w_end_date_month).alias("end_date_month_sum_premium"),
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
        F.min("premium").over(w_end_date_month).alias("end_date_month_min_premium"),
        F.rank().over(w_product_group_status_by_prem_3).alias("premium_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month), 2).alias("premium_vs_end_date_month_avg"),
        F.round(F.percent_rank().over(w_holder_id_product_group_by_h), 3).alias("holder_joined_on_percent_rank"),
        F.size(F.collect_set("status").over(w_end_date_month)).alias("distinct_status_in_end_date_month"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.abs(F.col("premium_dense_rank") - 6).alias("premium_dense_rank_distance"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.least(F.col("premium_rank"), F.col("holder_joined_on_decile")).alias("least_premium_rank_holder_joined_on_decile"),
    )
)

stats = (
    annotated
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        F.date_trunc("month", "holder_joined_on").alias("holder_joined_on_month"),
        "holder_joined_on_decile",
    )
    .agg(
        F.round(F.sum("least_premium_rank_holder_joined_on_decile") / F.nullif(F.sum("holder_joined_on_percent_rank"), F.lit(0)), 3).alias("least_premium_rank_holder_joined_on_decile_to_ho"),
        F.sum("premium_dense_rank").alias("total_premium_dense_rank"),
    )
)

windowed = (
    stats
    .select(
        "*",
        F.dense_rank().over(w_holder_joined_on_decile_hold).alias("total_premium_dense_rank_dense_rank"),
        F.max("holder_joined_on_decile").over(w_holder_joined_on_decile_hold_2).alias("max_holder_joined_on_decile_so_far"),
        F.rank().over(w_all_by_total_premium_dense_r).alias("total_premium_dense_rank_rank"),
    )
)

result = (
    windowed
    .select(
        "start_date_ym",
        "holder_joined_on_month",
        "total_premium_dense_rank",
        "total_premium_dense_rank_dense_rank",
        "max_holder_joined_on_decile_so_far",
        F.concat(F.col("start_date_ym"), F.lit("#"), F.col("total_premium_dense_rank").cast("string")).alias("start_date_ym_ref"),
    )
    .orderBy(
        F.col("start_date_ym").desc_nulls_first(),
        "total_premium_dense_rank_dense_rank",
    )
)
