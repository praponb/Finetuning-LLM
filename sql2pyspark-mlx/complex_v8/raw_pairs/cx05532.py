from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date_quarter").desc(), F.col("id").desc())

prepared_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

holder_id_stats = (
    prepared_policys
    .groupBy("holder_id")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

above_average = (
    prepared_policys.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

unique_rows = (
    above_average
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"))
)

with_stats = (
    unique_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.avg_claim_claimed_amount")
)

result = (
    with_stats
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "start_date_quarter",
        "premium_minus_median",
        "avg_claim_claimed_amount",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
    .orderBy(F.col("premium").desc(), "id")
    .limit(5)
)
