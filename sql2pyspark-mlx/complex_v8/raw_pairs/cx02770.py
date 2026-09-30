from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_holder_id_by_holder_risk_sco = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_holder_risk_sco_2 = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").desc())
w_holder_region_by_holder_risk = Window.partitionBy("holder_region").orderBy(F.col("holder_risk_score").desc(), "id")
w_product_by_end_date_moving_1 = Window.partitionBy("product").orderBy("end_date", "id").rowsBetween(-1, Window.currentRow)
w_holder_id_by_holder_risk_sco_3 = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").desc(), "id")
w_product_by_end_date_prior_6 = Window.partitionBy("product").orderBy("end_date", "id").rowsBetween(-6, -1)

recent_policys = df_policies.filter(~F.col("product").isin("AUTO", "LIFE"))

combined_details_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") >= 0.3)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

with_windows = (
    combined_details_holder
    .select(
        "id",
        "product",
        "end_date",
        "holder_risk_score",
        "holder_name",
        "holder_region",
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_holder_id), 2).alias("holder_risk_score_vs_holder_id_avg"),
        F.sum("holder_risk_score").over(w_holder_id_by_holder_risk_sco).alias("running_holder_risk_score"),
        F.rank().over(w_holder_id_by_holder_risk_sco_2).alias("holder_risk_score_rank"),
        F.lead("holder_risk_score", 1, 0).over(w_holder_region_by_holder_risk).alias("next_holder_risk_score"),
        F.round(F.avg("holder_risk_score").over(w_holder_id), 2).alias("holder_id_avg_holder_risk_score"),
        F.sum("holder_risk_score").over(w_product_by_end_date_moving_1).alias("holder_risk_score_sum_2"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id"),
        F.lead("holder_risk_score").over(w_holder_id_by_holder_risk_sco_3).alias("next_holder_risk_score_2"),
        F.max("holder_risk_score").over(w_product_by_end_date_prior_6).alias("holder_risk_score_max_6_prior"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_holder_risk_sco_3)).alias("days_since_prev_end_date"),
        F.max("holder_risk_score").over(w_holder_id).alias("holder_id_max_holder_risk_score"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("holder_risk_score_rank") == 1)
    .drop("holder_risk_score_rank")
)

stats = (
    top_per_group
    .filter(F.col("holder_risk_score_vs_holder_id_avg") < 0.4)
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_date_ym"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("holder_region") == "East", F.col("holder_risk_score_share_of_holder_id")).otherwise(0)).alias("holder_risk_score_share_of_holder_id_holder_regi"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)

result = (
    stats
    .select(
        "end_date_ym",
        "row_count",
        "holder_risk_score_share_of_holder_id_holder_regi",
        "product_list",
    )
)
