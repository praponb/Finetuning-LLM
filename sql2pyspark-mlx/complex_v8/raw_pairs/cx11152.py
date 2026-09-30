from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_holder_id = Window.partitionBy("holder_region", "holder_id")
w_start_date_year_holder_id_by = Window.partitionBy(F.year("start_date"), "holder_id").orderBy("start_date", "id")
w_all_by_holder_risk_score_tie = Window.orderBy(F.col("holder_risk_score").desc())
w_all_by_start_date = Window.orderBy("start_date", "id")
w_start_date_year_holder_id_by_2 = Window.partitionBy(F.year("start_date"), "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_ties = Window.orderBy("start_date")
w_end_date_year_holder_region = Window.partitionBy(F.year("end_date"), "holder_region")
w_holder_id_by_total_start_dat = Window.partitionBy("holder_id").orderBy("total_start_date_tercile", "holder_id")
w_holder_id = Window.partitionBy("holder_id")

recent = (
    df_policies
    .filter(F.col("product").isin("AUTO", "LIFE", "HOME"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

with_parent_holder = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.product_prefix",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    with_parent_holder
    .select(
        "*",
        F.min("holder_risk_score").over(w_holder_region_holder_id).alias("holder_region_holder_id_min_holder_risk_score"),
        F.round(F.cume_dist().over(w_start_date_year_holder_id_by), 3).alias("start_date_cume_dist"),
        F.rank().over(w_all_by_holder_risk_score_tie).alias("holder_risk_score_rank"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_start_date)).alias("days_since_prev_start_date"),
        F.row_number().over(w_start_date_year_holder_id_by).alias("rn"),
        F.ntile(3).over(w_all_by_start_date).alias("start_date_tercile"),
        F.nth_value("end_date", 2).over(w_start_date_year_holder_id_by_2).alias("second_end_date"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.sum("holder_risk_score").over(w_end_date_year_holder_region).alias("end_date_year_holder_region_sum_holder_risk_scor"),
    )
)

top_per_group = windowed.filter(F.col("rn") <= 5)

per_group = (
    top_per_group
    .groupBy("holder_id")
    .agg(
        F.sum("rn").alias("total_rn"),
        F.array_sort(F.collect_set("holder_region")).alias("holder_region_list"),
        F.sum("start_date_tercile").alias("total_start_date_tercile"),
        F.sum("start_date_cume_dist").alias("total_start_date_cume_dist"),
        F.round(F.sum("start_date_cume_dist") / F.nullif(F.sum("end_date_year_holder_region_sum_holder_risk_scor"), F.lit(0)), 3).alias("start_date_cume_dist_to_end_date_year_holder_reg"),
        F.sum("holder_risk_score").alias("total_holder_risk_score"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.lag("total_start_date_cume_dist", 2).over(w_holder_id_by_total_start_dat).alias("prev2_total_start_date_cume_dist"),
        F.sum("total_start_date_cume_dist").over(w_holder_id).alias("holder_id_sum_total_start_date_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "holder_id",
        "holder_region_list",
        "total_start_date_tercile",
        "total_start_date_cume_dist",
        "start_date_cume_dist_to_end_date_year_holder_reg",
        "prev2_total_start_date_cume_dist",
        "holder_id_sum_total_start_date_cume_dist",
        F.least(F.col("total_rn"), F.col("total_holder_risk_score")).alias("least_total_rn_total_holder_risk_score"),
        F.coalesce(F.col("total_start_date_tercile"), F.lit(0)).alias("total_start_date_tercile_filled"),
    )
    .orderBy(F.col("total_start_date_tercile_filled").desc_nulls_first())
)
