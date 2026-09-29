from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_claim_date_month_by_premium = Window.partitionBy(F.month("claim_date")).orderBy("premium", "id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

filtered = (
    in_window.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product").isNotNull()), "left_semi")
)

with_trends = (
    filtered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount",
        "product",
        F.ntile(5).over(w_policy_id_by_claimed_amount).alias("claimed_amount_quintile"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_claim_date_month_by_premium)).alias("approved_amount_change"),
    )
)

status_stats = (
    with_trends
    .groupBy("status")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

outliers = (
    with_trends.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "avg_claimed_amount",
        "claimed_amount_minus_avg",
        "claimed_amount",
        "approved_amount",
    )
    .unpivot(
        ids=["id"],
        values=["avg_claimed_amount", "claimed_amount_minus_avg", "claimed_amount", "approved_amount"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
