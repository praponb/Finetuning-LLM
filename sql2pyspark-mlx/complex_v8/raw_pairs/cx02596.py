from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_joined_on_year = Window.partitionBy("status", F.year("holder_joined_on"))
w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_end_date_ties = Window.orderBy("end_date")

cleaned = (
    df_policies
    .filter(F.col("holder_id") >= 12)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2025-09-02").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.status_clean",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_status_holder_joined_on_year).alias("rows_in_status_holder_joined_on_year"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_status_holder_joined_on_year), 2).alias("holder_risk_score_vs_status_holder_joined_on_yea"),
        F.last("end_date", ignorenulls=True).over(w_holder_id_status_by_end_date).alias("last_known_end_date"),
        F.last("holder_joined_on", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_holder_joined_on"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_status_holder_joined_on_year), F.lit(0)), 4).alias("holder_risk_score_share_of_status_holder_joined_"),
        F.ntile(5).over(w_all_by_end_date).alias("end_date_quintile"),
        F.count(F.lit(1)).over(w_all_by_end_date_running).alias("running_count"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "end_date",
        "status",
        "holder_joined_on",
        "holder_risk_score",
        "rows_in_status_holder_joined_on_year",
        "holder_risk_score_vs_status_holder_joined_on_yea",
        "last_known_holder_joined_on",
        "holder_risk_score_share_of_status_holder_joined_",
        "end_date_quintile",
        "running_count",
        "end_date_percent_rank",
    )
    .orderBy(
        F.col("end_date_percent_rank").asc(),
        F.col("holder_risk_score_share_of_status_holder_joined_").asc(),
        "id",
    )
    .limit(5)
)
