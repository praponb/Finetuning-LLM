from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_joined_on = Window.orderBy("days_since_joined_on", "id")
w_joined_on_year = Window.partitionBy(F.year("joined_on"))
w_all_by_joined_on = Window.orderBy("joined_on", "id")
w_all_by_days_since_joined_on_2 = Window.orderBy("days_since_joined_on", "id").rowsBetween(-6, -1)
w_all_by_risk_score = Window.orderBy("risk_score", "id")
w_days_since_joined_on_rank = Window.partitionBy("days_since_joined_on_rank")
w_seq_no_by_days_since_joined = Window.partitionBy("seq_no").orderBy("days_since_joined_on_rank", "id")
w_all_by_joined_on_year_max_ri = Window.orderBy("joined_on_year_max_risk_score")

filtered = (
    df_policyholders
    .filter((F.col("region") == "South") & (F.upper(F.trim(F.col("holder_name"))).like("L%")))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        F.date_format(F.col("joined_on"), "yyyy-MM-dd").alias("joined_on_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_days_since_joined_on).alias("days_since_joined_on_rank"),
        F.round(F.avg("days_since_joined_on").over(w_joined_on_year), 2).alias("joined_on_year_avg_days_since_joined_on"),
        F.max("days_since_joined_on").over(w_joined_on_year).alias("joined_on_year_max_days_since_joined_on"),
        F.count(F.lit(1)).over(w_joined_on_year).alias("rows_in_joined_on_year"),
        F.max("risk_score").over(w_joined_on_year).alias("joined_on_year_max_risk_score"),
        F.ntile(10).over(w_all_by_joined_on).alias("joined_on_decile"),
        F.round(F.avg("risk_score").over(w_all_by_days_since_joined_on_2), 2).alias("risk_score_avg_6_prior"),
        F.row_number().over(w_all_by_risk_score).alias("seq_no"),
    )
)

with_trends = (
    ranked
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "joined_on_label",
        "days_since_joined_on",
        "days_since_joined_on_rank",
        "joined_on_year_avg_days_since_joined_on",
        "joined_on_year_max_days_since_joined_on",
        F.count(F.lit(1)).over(w_days_since_joined_on_rank).alias("rows_in_days_since_joined_on_rank"),
        F.round(F.cume_dist().over(w_seq_no_by_days_since_joined), 3).alias("days_since_joined_on_rank_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_joined_on_year_max_ri), 3).alias("joined_on_year_max_risk_score_percent_rank"),
        F.dense_rank().over(w_all_by_joined_on_year_max_ri).alias("joined_on_year_max_risk_score_dense_rank"),
        F.dense_rank().over(w_seq_no_by_days_since_joined).alias("days_since_joined_on_rank_dense_rank"),
        F.round(F.col("joined_on_decile") / F.nullif(F.sum("joined_on_decile").over(w_days_since_joined_on_rank), F.lit(0)), 4).alias("joined_on_decile_share_of_days_since_joined_on_r"),
    )
)

result = (
    with_trends
    .filter(F.col("days_since_joined_on_rank") < 6)
    .select(
        "id",
        "risk_score",
        "joined_on",
        "joined_on_label",
        "days_since_joined_on",
        "joined_on_year_max_days_since_joined_on",
        "rows_in_days_since_joined_on_rank",
        "days_since_joined_on_rank_cume_dist",
        "joined_on_year_max_risk_score_percent_rank",
        "days_since_joined_on_rank_dense_rank",
        F.date_trunc("month", F.col("joined_on")).alias("joined_on_month"),
        (F.col("days_since_joined_on") - F.col("risk_score")).alias("days_since_joined_on_minus_risk_score"),
    )
    .orderBy(
        F.col("rows_in_days_since_joined_on_rank").desc_nulls_first(),
        F.col("joined_on_year_max_days_since_joined_on").asc(),
    )
)
