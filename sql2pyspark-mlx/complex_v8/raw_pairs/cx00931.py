from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_claim_count_ties = Window.partitionBy("product").orderBy(F.col("claim_count").desc())
w_holder_id = Window.partitionBy("holder_id")
w_product_by_claim_count_runni = Window.partitionBy("product").orderBy(F.col("claim_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("holder_id").orderBy(F.col("holder_joined_on").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter((F.col("holder_id") >= 15) | (F.col("product").isin("HOME", "AUTO", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_date",
    )
)

with_details = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name", F.col("p.risk_score").alias("holder_risk_score"))
)

windowed = (
    with_details
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "last_claim_date",
        "holder_name",
        F.round(F.percent_rank().over(w_product_by_claim_count_ties), 3).alias("claim_count_percent_rank"),
        F.max("holder_risk_score").over(w_holder_id).alias("holder_id_max_holder_risk_score"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_holder_id), F.lit(0)), 4).alias("claim_count_share_of_holder_id"),
        F.round(F.avg("holder_risk_score").over(w_holder_id), 2).alias("holder_id_avg_holder_risk_score"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.count(F.lit(1)).over(w_product_by_claim_count_runni).alias("running_count"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

combined_details_holder = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.claim_count_percent_rank",
        "s.holder_id_max_holder_risk_score",
        "s.claim_count_share_of_holder_id",
        "s.holder_id_avg_holder_risk_score",
        "s.rows_in_holder_id",
        "s.running_count",
        "s.product_prefix",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

latest_per_key = (
    combined_details_holder
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "claim_count_percent_rank",
        "holder_id_max_holder_risk_score",
        "claim_count_share_of_holder_id",
        "holder_id_avg_holder_risk_score",
        "rows_in_holder_id",
        "holder_joined_on",
        (F.floor(F.col("running_count") / 5) * 5).alias("running_count_bucket"),
    )
    .orderBy("claim_count_percent_rank")
)
