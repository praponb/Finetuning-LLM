from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_or_unknown_by_holder = Window.partitionBy("product_or_unknown").orderBy("holder_risk_score", "id").rowsBetween(-1, Window.currentRow)
w_product_or_unknown_by_holder_2 = Window.partitionBy("product_or_unknown").orderBy("holder_risk_score", "id")
w_all_by_holder_risk_score_tie = Window.orderBy(F.col("holder_risk_score").desc_nulls_first())
w_all_by_holder_risk_score = Window.orderBy(F.col("holder_risk_score").desc_nulls_first(), "id")
w_product_holder_id_by_holder = Window.partitionBy("product", "holder_id").orderBy("holder_risk_score", "id")
w_product = Window.partitionBy("product")
w_holder_id_by_holder_risk_sco = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_holder_id_by_holder_risk_sco_2 = Window.partitionBy("holder_id").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_holder_risk_score_ran = Window.partitionBy("n_rows", "holder_risk_score_rank")
w_holder_risk_score_rank_n_row = Window.partitionBy("holder_risk_score_rank", "n_rows").orderBy("holder_risk_score_seq_product_health", "holder_risk_score_rank").rowsBetween(-6, -1)
w_holder_risk_score_rank_n_row_2 = Window.partitionBy("holder_risk_score_rank", "n_rows").orderBy("holder_risk_score_seq_product_health", "holder_risk_score_rank")

base = (
    df_policies
    .filter(~F.col("status").isin("CANCELLED", "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

joined = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.product_or_unknown",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.max("holder_risk_score").over(w_product_or_unknown_by_holder).alias("holder_risk_score_max_2"),
        F.lag("holder_risk_score").over(w_product_or_unknown_by_holder_2).alias("prev_holder_risk_score"),
        F.rank().over(w_all_by_holder_risk_score_tie).alias("holder_risk_score_rank"),
        F.lag("holder_risk_score").over(w_all_by_holder_risk_score).alias("prev_holder_risk_score_2"),
        F.row_number().over(w_product_holder_id_by_holder).alias("holder_risk_score_seq"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_product), F.lit(0)), 4).alias("holder_risk_score_share_of_product"),
        F.min("holder_risk_score").over(w_product).alias("product_min_holder_risk_score"),
        F.max("holder_risk_score").over(w_holder_id_by_holder_risk_sco).alias("holder_risk_score_max_2_prior"),
        F.sum("holder_risk_score").over(w_product).alias("product_sum_holder_risk_score"),
        F.first("holder_risk_score").over(w_holder_id_by_holder_risk_sco_2).alias("first_holder_risk_score"),
    )
)

first_rows = sequenced.filter(F.col("holder_risk_score_rank") == 1)

grouped = (
    first_rows
    .groupBy("holder_risk_score_rank")
    .agg(
        F.sum(F.when(F.col("product") == "HEALTH", F.col("holder_risk_score_seq")).otherwise(0)).alias("holder_risk_score_seq_product_health"),
        F.round(F.sum("prev_holder_risk_score"), 2).alias("total_prev_holder_risk_score"),
        F.count("*").alias("n_rows"),
    )
)

with_windows = (
    grouped
    .select(
        "holder_risk_score_rank",
        "holder_risk_score_seq_product_health",
        "total_prev_holder_risk_score",
        "n_rows",
        F.count(F.lit(1)).over(w_n_rows_holder_risk_score_ran).alias("rows_in_n_rows_holder_risk_score_rank"),
        F.sum("n_rows").over(w_holder_risk_score_rank_n_row).alias("n_rows_sum_6_prior"),
        F.dense_rank().over(w_holder_risk_score_rank_n_row_2).alias("holder_risk_score_seq_product_health_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "holder_risk_score_rank",
        "total_prev_holder_risk_score",
        "n_rows",
        "rows_in_n_rows_holder_risk_score_rank",
        "n_rows_sum_6_prior",
        F.round(F.col("n_rows_sum_6_prior") / F.nullif(F.col("holder_risk_score_rank"), F.lit(0)), 3).alias("n_rows_sum_6_prior_per_holder_risk_score_rank"),
    )
    .orderBy(
        F.col("rows_in_n_rows_holder_risk_score_rank").desc(),
        F.col("n_rows_sum_6_prior").desc(),
    )
)
