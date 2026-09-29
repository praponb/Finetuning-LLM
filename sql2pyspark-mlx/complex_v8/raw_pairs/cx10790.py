from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_plus_12m_year = Window.partitionBy(F.year("start_date_plus_12m"))
w_status_holder_id_by_holder_r = Window.partitionBy("status", "holder_id").orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_holder_risk_score = Window.partitionBy("product").orderBy(F.col("holder_risk_score").desc(), "id")
w_status_holder_id_by_holder_r_2 = Window.partitionBy("status", "holder_id").orderBy("holder_risk_score", "id")
w_all_by_start_date_plus_12m = Window.orderBy("start_date_plus_12m", "id")
w_status_holder_id_by_holder_r_3 = Window.partitionBy("status", "holder_id").orderBy("holder_risk_score")
w_status_holder_id_by_holder_r_4 = Window.partitionBy("status", "holder_id").orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_holder_risk_score_2 = Window.partitionBy("product").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_year_by_start_date = Window.partitionBy(F.year("end_date")).orderBy("start_date_plus_12m", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_status_holder_id_by_holder_r_5 = Window.partitionBy("status", "holder_id").orderBy("holder_risk_score", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-07-11").cast("date")) & (F.col("start_date").between(F.lit("2024-11-20").cast("date"), F.lit("2025-07-07").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
    )
)

joined_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.start_date_plus_12m",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    joined_holder
    .select(
        "*",
        F.count(F.lit(1)).over(w_start_date_plus_12m_year).alias("rows_in_start_date_plus_12m_year"),
        F.first("start_date_plus_12m", ignorenulls=True).over(w_status_holder_id_by_holder_r).alias("first_known_start_date_plus_12m"),
        F.dense_rank().over(w_product_by_holder_risk_score).alias("holder_risk_score_dense_rank"),
        F.lead("end_date", 2).over(w_status_holder_id_by_holder_r_2).alias("next2_end_date"),
        F.lead("end_date").over(w_all_by_start_date_plus_12m).alias("next_end_date"),
        F.max("holder_risk_score").over(w_start_date_plus_12m_year).alias("start_date_plus_12m_year_max_holder_risk_score"),
        F.round(F.percent_rank().over(w_status_holder_id_by_holder_r_3), 3).alias("holder_risk_score_percent_rank"),
        F.nth_value("end_date", 2).over(w_status_holder_id_by_holder_r_4).alias("second_end_date"),
        F.max("holder_risk_score").over(w_product_by_holder_risk_score_2).alias("max_holder_risk_score_so_far"),
        F.round(F.percent_rank().over(w_end_date_year_by_start_date), 3).alias("start_date_plus_12m_percent_rank"),
        F.rank().over(w_status_holder_id_by_holder_r_3).alias("holder_risk_score_rank"),
        F.size(F.collect_set("status").over(w_holder_id_product)).alias("distinct_status_in_holder_id_product"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_start_date_plus_12m_year), 2).alias("holder_risk_score_vs_start_date_plus_12m_year_av"),
        F.round(F.avg("holder_risk_score").over(w_status_holder_id_by_holder_r_5), 2).alias("holder_risk_score_avg_4"),
        F.round(F.avg("holder_risk_score").over(w_holder_id_product), 2).alias("holder_id_product_avg_holder_risk_score"),
    )
)

labeled = (
    with_trends
    .filter(F.col("start_date_plus_12m_percent_rank").between(0.1, 0.6))
    .select(
        "*",
        F.round(F.col("holder_risk_score_avg_4") / F.nullif(F.col("holder_risk_score"), F.lit(0)), 3).alias("holder_risk_score_avg_4_per_holder_risk_score"),
        F.abs(F.col("holder_risk_score_avg_4") - 0.5).alias("holder_risk_score_avg_4_distance"),
        F.greatest(F.col("holder_risk_score_avg_4"), F.col("holder_risk_score_rank")).alias("greatest_holder_risk_score_avg_4_holder_risk_sco"),
    )
)

result = (
    labeled
    .select(
        "id",
        "end_date",
        "status",
        "start_date_plus_12m",
        "holder_risk_score",
        "first_known_start_date_plus_12m",
        "next2_end_date",
        "start_date_plus_12m_year_max_holder_risk_score",
        "holder_risk_score_percent_rank",
        "second_end_date",
        "max_holder_risk_score_so_far",
        "start_date_plus_12m_percent_rank",
        "holder_risk_score_rank",
        "distinct_status_in_holder_id_product",
        "holder_risk_score_vs_start_date_plus_12m_year_av",
        "holder_risk_score_avg_4",
        "holder_id_product_avg_holder_risk_score",
        "holder_risk_score_avg_4_distance",
        "greatest_holder_risk_score_avg_4_holder_risk_sco",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.abs(F.col("holder_risk_score_avg_4") - 0.5).alias("holder_risk_score_avg_4_distance_2"),
    )
)
