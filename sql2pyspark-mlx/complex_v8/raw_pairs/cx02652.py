from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_product_by_sta = Window.partitionBy("holder_region", "product").orderBy("start_date", "id")
w_holder_region_product_by_sta_2 = Window.partitionBy("holder_region", "product").orderBy("start_date", "id").rowsBetween(-1, -1)
w_holder_region_product_by_sta_3 = Window.partitionBy("holder_region", "product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_start_date_year_by = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("end_date").desc())
w_holder_segment_by_start_date = Window.partitionBy("holder_segment").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment_by_start_date_2 = Window.partitionBy("holder_segment").orderBy("start_date", "id")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy("premium_adj")
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy("premium_adj", "id")
w_premium_adj_dense_rank_by_da = Window.partitionBy("premium_adj_dense_rank").orderBy("days_since_prev_end_date", "id")
w_premium_adj_dense_rank_by_da_2 = Window.partitionBy("premium_adj_dense_rank").orderBy("days_since_prev_end_date")
w_end_date_rank_holder_id = Window.partitionBy("end_date_rank", "holder_id")

scoped_policys = (
    df_policies
    .filter(F.col("premium") < 3465.4)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

with_details_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.year(F.col("p.joined_on")) == 2024), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_adj",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

with_windows = (
    with_details_holder
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "premium_adj",
        F.ntile(5).over(w_holder_region_product_by_sta).alias("start_date_quintile"),
        F.round(F.avg("premium").over(w_holder_region_product_by_sta_2), 2).alias("premium_avg_1_prior"),
        F.sum("premium_adj").over(w_holder_region_product_by_sta_3).alias("running_premium_adj"),
        F.rank().over(w_holder_id_start_date_year_by).alias("end_date_rank"),
        F.nth_value("end_date", 2).over(w_holder_segment_by_start_date).alias("second_end_date"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_holder_segment_by_start_date_2)).alias("premium_adj_change"),
        F.dense_rank().over(w_status_holder_id_by_premium).alias("premium_adj_dense_rank"),
        F.lead("premium", 2).over(w_holder_region_product_by_sta).alias("next2_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_status_holder_id_by_premium_2)).alias("days_since_prev_end_date"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.lead("second_end_date").over(w_premium_adj_dense_rank_by_da).alias("next_second_end_date"),
        F.round(F.percent_rank().over(w_premium_adj_dense_rank_by_da_2), 3).alias("days_since_prev_end_date_percent_rank"),
        F.count(F.lit(1)).over(w_end_date_rank_holder_id).alias("rows_in_end_date_rank_holder_id"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "premium_adj",
        "premium_avg_1_prior",
        "running_premium_adj",
        "next2_premium",
        "days_since_prev_end_date",
        "next_second_end_date",
        "days_since_prev_end_date_percent_rank",
        "rows_in_end_date_rank_holder_id",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.abs(F.col("start_date_quintile") - 6).alias("start_date_quintile_distance"),
    )
    .orderBy(F.col("start_date_quintile_distance").desc())
)
