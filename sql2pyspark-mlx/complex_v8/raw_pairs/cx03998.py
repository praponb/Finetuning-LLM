from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product = Window.partitionBy("policy_product")
w_prior_rows_60d_by_policy_pre = Window.partitionBy("prior_rows_60d").orderBy(F.col("policy_premium").desc())

in_window = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

joined_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") >= F.lit("2024-08-27").cast("date")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.prior_rows_60d",
        "s.prior_approved_amount_60d",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

windowed = (
    joined_policy
    .select(
        "*",
        F.max("prior_rows_60d").over(w_policy_product).alias("policy_product_max_prior_rows_60d"),
        F.dense_rank().over(w_prior_rows_60d_by_policy_pre).alias("policy_premium_dense_rank"),
        F.round(F.avg("policy_premium").over(w_policy_product), 2).alias("policy_product_avg_policy_premium"),
    )
)

derived = (
    windowed
    .select(
        "*",
        F.round(F.col("policy_premium") * 1.1, 2).alias("policy_premium_adj"),
        F.abs(F.col("prior_rows_60d") - 4).alias("prior_rows_60d_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("policy_premium_dense_rank").alias("m_policy_premium_dense_rank"),
        F.col("prior_rows_60d").alias("prior_rows_60d_v"),
        F.col("prior_rows_60d_distance").alias("prior_rows_60d_distance_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_policy_premium_dense_rank", "prior_rows_60d_v", "prior_rows_60d_distance_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
