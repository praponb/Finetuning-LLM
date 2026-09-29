from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_risk_score_tie = Window.orderBy("holder_risk_score")
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id")

recent_policys = (
    df_policies
    .filter(F.col("product") != "AUTO")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

with_parent = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

without_matches = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

with_windows = (
    without_matches
    .select(
        "*",
        F.rank().over(w_all_by_holder_risk_score_tie).alias("holder_risk_score_rank"),
        F.last("end_date", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_end_date"),
        F.rank().over(w_all_by_holder_risk_score).alias("holder_risk_score_rank_2"),
    )
)

leaders = (
    with_windows
    .filter((F.col("holder_risk_score_rank") == 1) & (F.col("holder_risk_score_rank_2") >= 4))
)

result = (
    leaders
    .select(
        "id",
        "end_date",
        "status",
        "holder_joined_on",
        "holder_risk_score_rank",
        "last_known_end_date",
        "holder_risk_score_rank_2",
    )
    .orderBy(
        F.col("end_date").desc(),
        F.col("last_known_end_date").desc_nulls_first(),
    )
)
