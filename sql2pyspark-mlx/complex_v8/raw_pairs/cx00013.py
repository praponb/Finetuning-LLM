from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_latest = Window.partitionBy("status").orderBy(F.col("start_date").desc(), F.col("id").desc())

prepared_policys = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
    )
)

screened = (
    prepared_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "REJECTED") & (F.col("c.approved_amount") < 14377.6), "left_semi")
)

combined_details = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    combined_details
    .withColumn("premium_distance_vs_holder_id_avg", F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_holder_id), 2))
    .withColumn("holder_risk_score_vs_holder_id_avg", F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_holder_id), 2))
)

latest_per_key = (
    with_windows
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "holder_id",
        "status",
        "days_start_date_to_end_date",
        "premium_distance",
        "holder_risk_score",
        "premium_distance_vs_holder_id_avg",
        "holder_risk_score_vs_holder_id_avg",
        F.when(F.col("premium") >= 3532.7, "A").when(F.col("premium") >= 2033.8, "B").otherwise("C").alias("premium_tier"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
    .orderBy(F.col("id").asc(), F.col("holder_id").asc())
)
