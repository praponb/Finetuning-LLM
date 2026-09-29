from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_plus_3m_month = Window.partitionBy(F.date_trunc("month", "end_date_plus_3m"))
w_all_by_days_start_date_to_en = Window.orderBy(F.col("days_start_date_to_end_date").desc())
w_all_by_days_start_date_to_en_2 = Window.orderBy(F.col("days_start_date_to_end_date").desc(), "id")
w_all_by_days_start_date_to_en_3 = Window.orderBy(F.col("days_start_date_to_end_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_distinct_holder_id_in_end_da = Window.partitionBy("distinct_holder_id_in_end_date_plus_3m_month", F.month("end_date_plus_3m")).orderBy("premium_max_3")
w_holder_id_end_date_plus_3m_m = Window.partitionBy("holder_id", F.month("end_date_plus_3m")).orderBy("premium", "id")

base_policys = (
    df_policies
    .filter(F.col("status") != "CANCELLED")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.add_months(F.col("end_date"), 3).alias("end_date_plus_3m"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

sequenced = (
    base_policys
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_end_date_plus_3m_month)).alias("distinct_holder_id_in_end_date_plus_3m_month"),
        F.dense_rank().over(w_all_by_days_start_date_to_en).alias("days_start_date_to_end_date_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_days_start_date_to_en_2), 3).alias("days_start_date_to_end_date_cume_dist"),
        F.lead("premium", 2).over(w_all_by_days_start_date_to_en_2).alias("next2_premium"),
        F.max("premium").over(w_all_by_days_start_date_to_en_3).alias("premium_max_3"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "days_since_start_date",
        "distinct_holder_id_in_end_date_plus_3m_month",
        "days_start_date_to_end_date_dense_rank",
        "days_start_date_to_end_date_cume_dist",
        "premium_max_3",
        F.round(F.col("days_start_date_to_end_date") / F.nullif(F.sum("days_start_date_to_end_date").over(w_holder_id), F.lit(0)), 4).alias("days_start_date_to_end_date_share_of_holder_id"),
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_holder_id), 2).alias("days_since_start_date_vs_holder_id_avg"),
        F.rank().over(w_distinct_holder_id_in_end_da).alias("premium_max_3_rank"),
        F.round(F.percent_rank().over(w_distinct_holder_id_in_end_da), 3).alias("premium_max_3_percent_rank"),
        F.sum("days_since_start_date").over(w_holder_id).alias("holder_id_sum_days_since_start_date"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_end_date_plus_3m_m)).alias("days_since_prev_end_date"),
    )
)

labeled = (
    sequenced_2
    .select(
        "*",
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
        F.coalesce(F.col("premium_max_3"), F.lit(0.0)).alias("premium_max_3_filled"),
    )
)

screened = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") >= F.lit("2024-06-29").cast("date")) & (F.col("p.region").isNotNull()), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "distinct_holder_id_in_end_date_plus_3m_month",
        "days_start_date_to_end_date_dense_rank",
        "days_start_date_to_end_date_cume_dist",
        "days_start_date_to_end_date_share_of_holder_id",
        "days_since_start_date_vs_holder_id_avg",
        "premium_max_3_rank",
        "holder_id_sum_days_since_start_date",
        "days_since_prev_end_date",
        "end_date_quarter",
        "premium_max_3_filled",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
    )
    .orderBy("premium_max_3_filled", "id")
    .limit(10)
)
