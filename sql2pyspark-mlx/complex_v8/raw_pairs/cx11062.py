from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month = Window.partitionBy("holder_id", F.date_trunc("month", "end_date"))
w_start_date_year_status_by_st = Window.partitionBy(F.year("start_date"), "status").orderBy("start_date", "id").rowsBetween(-3, -1)
w_holder_segment = Window.partitionBy("holder_segment")
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy("end_date")
w_holder_segment_holder_id_by = Window.partitionBy("holder_segment", "holder_id").orderBy("premium_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_start_date_year_status_by_st_2 = Window.partitionBy(F.year("start_date"), "status").orderBy("start_date", "id")
w_start_date_year_status_by_st_3 = Window.partitionBy(F.year("start_date"), "status").orderBy("start_date")
w_all_by_start_date_2 = Window.orderBy("start_date", "id")
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").asc_nulls_last())

base_policys = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2026) & (F.col("premium").between(2033.8, 3544.1)))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
)

joined = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isin("East", "West")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_adj",
        F.col("p.segment").alias("holder_segment"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_holder_id_end_date_month), 2).alias("premium_adj_vs_holder_id_end_date_month_avg"),
        F.max("premium_adj").over(w_start_date_year_status_by_st).alias("premium_adj_max_3_prior"),
        F.size(F.collect_set("holder_id").over(w_holder_segment)).alias("distinct_holder_id_in_holder_segment"),
        F.rank().over(w_holder_id_by_end_date_ties).alias("end_date_rank"),
        F.count(F.lit(1)).over(w_holder_segment).alias("rows_in_holder_segment"),
        F.min("premium_adj").over(w_holder_segment_holder_id_by).alias("min_premium_adj_so_far"),
        F.count(F.lit(1)).over(w_holder_id_end_date_month).alias("rows_in_holder_id_end_date_month"),
        F.ntile(4).over(w_holder_id_by_end_date).alias("end_date_quartile"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_start_date_year_status_by_st_2)).alias("days_since_prev_start_date"),
        F.rank().over(w_start_date_year_status_by_st_3).alias("start_date_rank"),
        F.lead("premium_adj").over(w_all_by_start_date_2).alias("next_premium_adj"),
        F.count(F.lit(1)).over(w_holder_id_by_end_date_runnin).alias("running_count"),
        F.first("premium_adj", ignorenulls=True).over(w_holder_segment_holder_id_by).alias("first_known_premium_adj"),
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_holder_segment), 2).alias("premium_adj_vs_holder_segment_avg"),
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        "premium_adj",
        "premium_adj_vs_holder_id_end_date_month_avg",
        "premium_adj_max_3_prior",
        "distinct_holder_id_in_holder_segment",
        "rows_in_holder_segment",
        "min_premium_adj_so_far",
        "rows_in_holder_id_end_date_month",
        "end_date_quartile",
        "start_date_cume_dist",
        "days_since_prev_start_date",
        "start_date_rank",
        "running_count",
        "first_known_premium_adj",
        "premium_adj_vs_holder_segment_avg",
        "end_date_rank_2",
        F.when(F.col("first_known_premium_adj") >= 5675.0, "large").when(F.col("first_known_premium_adj") >= 2390.0, "medium").otherwise("small").alias("first_known_premium_adj_tier"),
    )
    .orderBy(F.col("premium_adj_vs_holder_id_end_date_month_avg").asc_nulls_last())
)
