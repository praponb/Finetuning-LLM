from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_month_holder = Window.partitionBy(F.month("end_date_month"), "holder_id")
w_status_group_by_holder_risk = Window.partitionBy("status_group").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_status_group = Window.partitionBy("product", "status_group")
w_status_group_holder_id = Window.partitionBy("status_group", "holder_id")
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_group_product_by_end = Window.partitionBy("status_group", "product").orderBy(F.col("end_date_month").desc_nulls_first(), "id")
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy("end_date")
w_all_by_end_date_month_ties = Window.orderBy("end_date_month")
w_all_by_holder_risk_score = Window.orderBy(F.col("holder_risk_score").asc_nulls_last(), "id")
w_end_date_rank = Window.partitionBy("end_date_rank")
w_end_date_rank_holder_id_by_e = Window.partitionBy("end_date_rank", "holder_id").orderBy(F.col("end_date_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_group_holder_id_by_st = Window.partitionBy("status_group", "holder_id").orderBy(F.col("status_group_holder_id_avg_holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((F.col("holder_id") >= 2) & (F.col("start_date").between(F.lit("2024-01-25").cast("date"), F.lit("2025-10-13").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.when(F.col("status").isin("ACTIVE", "CANCELLED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)

enriched_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status_group",
        "s.end_date_month",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

windowed = (
    enriched_holder
    .select(
        "id",
        "holder_id",
        "status_group",
        "end_date_month",
        "holder_name",
        F.max("holder_risk_score").over(w_end_date_month_month_holder).alias("end_date_month_month_holder_id_max_holder_risk_s"),
        F.last("end_date", ignorenulls=True).over(w_status_group_by_holder_risk).alias("last_known_end_date"),
        F.max("holder_risk_score").over(w_product_status_group).alias("product_status_group_max_holder_risk_score"),
        F.round(F.avg("holder_risk_score").over(w_status_group_holder_id), 2).alias("status_group_holder_id_avg_holder_risk_score"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_end_date)).alias("days_since_prev_end_date"),
        F.count(F.lit(1)).over(w_end_date_month_month_holder).alias("rows_in_end_date_month_month_holder_id"),
        F.last("end_date", ignorenulls=True).over(w_holder_id_by_end_date_runnin).alias("last_known_end_date_2"),
        F.round(F.percent_rank().over(w_status_group_product_by_end), 3).alias("end_date_month_percent_rank"),
        F.rank().over(w_holder_id_by_end_date_ties).alias("end_date_rank"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_status_group_product_by_end)).alias("days_since_prev_end_date_2"),
        F.round(F.percent_rank().over(w_all_by_end_date_month_ties), 3).alias("end_date_month_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_holder_risk_score), 3).alias("holder_risk_score_cume_dist"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "holder_id",
        "status_group",
        "end_date_month",
        "end_date_month_month_holder_id_max_holder_risk_s",
        "last_known_end_date",
        "product_status_group_max_holder_risk_score",
        "days_since_prev_end_date",
        "rows_in_end_date_month_month_holder_id",
        "last_known_end_date_2",
        "end_date_month_percent_rank",
        "end_date_month_percent_rank_2",
        "holder_risk_score_cume_dist",
        F.round(F.avg("end_date_rank").over(w_end_date_rank), 2).alias("end_date_rank_avg_end_date_rank"),
        F.round(F.col("holder_risk_score_cume_dist") / F.nullif(F.sum("holder_risk_score_cume_dist").over(w_end_date_rank), F.lit(0)), 4).alias("holder_risk_score_cume_dist_share_of_end_date_ra"),
        F.first("days_since_prev_end_date", ignorenulls=True).over(w_end_date_rank_holder_id_by_e).alias("first_known_days_since_prev_end_date"),
        F.min("days_since_prev_end_date_2").over(w_status_group_holder_id_by_st).alias("min_days_since_prev_end_date_2_so_far"),
        F.size(F.collect_set("status_group").over(w_end_date_rank)).alias("distinct_status_group_in_end_date_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "status_group",
        "end_date_month",
        "end_date_month_month_holder_id_max_holder_risk_s",
        "last_known_end_date",
        "days_since_prev_end_date",
        "rows_in_end_date_month_month_holder_id",
        "end_date_month_percent_rank",
        "holder_risk_score_cume_dist_share_of_end_date_ra",
        "first_known_days_since_prev_end_date",
        "min_days_since_prev_end_date_2_so_far",
        "distinct_status_group_in_end_date_rank",
        F.to_date(F.col("end_date_month")).alias("end_date_month_date"),
    )
    .orderBy(F.col("first_known_days_since_prev_end_date").desc_nulls_first())
)
