from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type").orderBy(F.col("claim_date_dayofweek").desc(), F.col("id").desc())

base_claims = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) | (F.month(F.col("reported_at")).isin(2, 3)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

qualified = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") < F.lit("2025-02-18 14:00:00").cast("timestamp")), "left_semi")
)

deduped = (
    qualified
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .rollup("claim_date_dayofweek", "claim_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("claim_type") == "fire", 1)) / F.count("*"), 1).alias("pct_claim_type_fire"),
    )
    .orderBy(
        F.col("claim_date_dayofweek").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
