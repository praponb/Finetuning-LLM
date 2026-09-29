from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_risk_score_ful = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_holder_risk_score_tie = Window.orderBy("holder_risk_score")
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id")
w_holder_id_status = Window.partitionBy("holder_id", "status")

recent_policys = (
    df_policies
    .filter(F.col("holder_id") >= 9)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
    )
)

with_parent_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    with_parent_holder
    .select(
        "id",
        "product",
        "status",
        F.nth_value("holder_risk_score", 2).over(w_all_by_holder_risk_score_ful).alias("second_holder_risk_score"),
        F.round(F.cume_dist().over(w_all_by_holder_risk_score_tie), 3).alias("holder_risk_score_cume_dist"),
        F.ntile(5).over(w_all_by_holder_risk_score).alias("holder_risk_score_quintile"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id_status), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id_status"),
        F.sum("holder_risk_score").over(w_holder_id_status).alias("holder_id_status_sum_holder_risk_score"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_risk_score_share_of_holder_id_status",
        "holder_risk_score_cume_dist",
        "second_holder_risk_score",
        "holder_id_status_sum_holder_risk_score",
    )
    .unpivot(
        ids=["id"],
        values=["holder_risk_score_share_of_holder_id_status", "holder_risk_score_cume_dist", "second_holder_risk_score", "holder_id_status_sum_holder_risk_score"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .orderBy("id", "measure")
)
