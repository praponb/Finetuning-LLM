from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_adjuster_id").orderBy(F.col("premium").desc(), F.col("id").desc(), F.col("claim_id").desc())
w_all_by_premium_running = Window.orderBy("premium", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_id_by_claim_date_prior = Window.partitionBy("claim_id").orderBy("claim_date", "id", "claim_id").rowsBetween(-1, -1)
w_all_by_premium = Window.orderBy("premium", "id", "claim_id")
w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")

valid = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("status").isin("EXPIRED", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

with_children = (
    valid.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

latest = (
    with_children
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    latest
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_premium_running).alias("running_count"),
        F.round(F.avg("premium").over(w_claim_id_by_claim_date_prior), 2).alias("premium_avg_1_prior"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_premium)).alias("days_since_prev_claim_date"),
        F.last("claim_date", ignorenulls=True).over(w_all_by_claim_date_running).alias("last_known_claim_date"),
        F.round(F.avg("premium").over(w_holder_id), 2).alias("holder_id_avg_premium"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.claim_id",
        "s.claim_date",
        "s.claim_adjuster_id",
        "s.running_count",
        "s.premium_avg_1_prior",
        "s.last_known_claim_date",
        "s.holder_id_avg_premium",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "claim_id",
        "running_count",
        "last_known_claim_date",
        "holder_risk_score",
        "holder_segment",
        "adjuster_team",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
    .orderBy(F.col("running_count").desc(), "id", "claim_id")
    .limit(20)
)
