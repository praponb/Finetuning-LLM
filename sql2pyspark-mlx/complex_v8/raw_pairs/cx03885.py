from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_holder_id_by_holder_joined_o = Window.partitionBy("holder_id").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_holder_id_by_holder_joined_o_2 = Window.partitionBy("holder_id").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest_2 = Window.partitionBy("holder_id", "claim_adjuster_id").orderBy(F.col("first_known_premium").desc(), F.col("id").desc(), F.col("claim_id").desc())

filtered_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-06-19").cast("date")) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

deduped = (
    filtered_policys
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_holder = (
    deduped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

with_trends = (
    with_details_holder
    .select(
        "*",
        F.first("premium", ignorenulls=True).over(w_holder_id_by_holder_joined_o).alias("first_known_premium"),
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.first("start_date", ignorenulls=True).over(w_holder_id_by_holder_joined_o).alias("first_known_start_date"),
        F.first("end_date").over(w_holder_id_by_holder_joined_o_2).alias("first_end_date"),
    )
)

detail_rows_claims = (
    with_trends.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-03-02").cast("date")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.holder_joined_on",
        "s.first_known_premium",
        "s.first_known_start_date",
        "s.first_end_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

latest = (
    detail_rows_claims
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details_holder = (
    latest.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.holder_joined_on",
        "s.first_known_start_date",
        "s.claim_id",
        "s.claim_adjuster_id",
        F.col("p.risk_score").alias("holder_risk_score"),
        "a.adjuster_name",
    )
)

result = (
    combined_details_holder
    .select(
        "id",
        "start_date",
        "holder_joined_on",
        "holder_risk_score",
        "adjuster_name",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
    )
    .orderBy(F.col("adjuster_name_slug").desc())
)
