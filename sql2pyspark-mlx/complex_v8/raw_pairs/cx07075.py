from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_adjuster_id_status_by_premiu = Window.partitionBy("adjuster_id", "status").orderBy("premium", "id")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_status_by_premiu_2 = Window.partitionBy("adjuster_id", "status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.max("claimed_amount").over(w_status_adjuster_id).alias("status_adjuster_id_max_claimed_amount"),
        F.dense_rank().over(w_adjuster_id_status_by_premiu).alias("premium_dense_rank"),
        F.first("claim_date", ignorenulls=True).over(w_all_by_premium_running).alias("first_known_claim_date"),
        F.last("claim_date", ignorenulls=True).over(w_adjuster_id_status_by_premiu_2).alias("last_known_claim_date"),
        F.last("claimed_amount", ignorenulls=True).over(w_adjuster_id_status_by_premiu_2).alias("last_known_claimed_amount"),
    )
)

with_parent = (
    with_windows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "product",
        "premium",
        "status_adjuster_id_max_claimed_amount",
        "premium_dense_rank",
        "first_known_claim_date",
        "last_known_claim_date",
        "last_known_claimed_amount",
        "adjuster_team",
    )
    .orderBy(
        "adjuster_team",
        F.col("status_adjuster_id_max_claimed_amount").asc_nulls_last(),
    )
)
