from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claim_date = Window.partitionBy("status").orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_date_ties = Window.orderBy("claim_date")

scoped = (
    df_claims
    .filter((F.col("adjuster_id") <= 4) & (F.col("claimed_amount").between(11235.6, 40639.7)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") < 31752.9), "left_semi")
)

metrics = (
    without_matches
    .select(
        "id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.round(F.percent_rank().over(w_status_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.row_number().over(w_status_by_claim_date).alias("seq_no"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
    )
)

summary = (
    metrics
    .groupBy(F.year("claim_date").alias("claim_date_year"))
    .agg(
        F.round(F.sum("claim_date_percent_rank") / F.nullif(F.sum("seq_no"), F.lit(0)), 3).alias("claim_date_percent_rank_to_seq_no_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("claim_type") == "delay", 1)).alias("n_claim_type_delay"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum("claim_date_dense_rank").alias("total_claim_date_dense_rank"),
    )
    .filter(F.col("total_claim_date_dense_rank") > 3)
)

annotated = (
    summary
    .filter(F.col("total_claim_date_dense_rank") <= 45)
    .select(
        "claim_date_year",
        "claim_date_percent_rank_to_seq_no_ratio",
        "distinct_ids",
        "n_claim_type_delay",
        "claim_type_list",
        "total_claim_date_dense_rank",
        F.when(F.col("distinct_ids") >= 7, "large").when(F.col("distinct_ids") >= 3, "medium").otherwise("small").alias("distinct_ids_tier"),
    )
)

result = (
    annotated
    .select("claim_date_year", "n_claim_type_delay", "distinct_ids")
    .unpivot(
        ids=["claim_date_year"],
        values=["n_claim_type_delay", "distinct_ids"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("claim_date_year", "field")
)
