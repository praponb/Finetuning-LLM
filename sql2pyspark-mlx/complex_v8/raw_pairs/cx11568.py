from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "product").orderBy(F.col("holder_joined_on").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2026-09-19").cast("date"))) & (F.col("holder_id") <= 4))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

combined_details_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

deduped = (
    combined_details_holder
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_holder = (
    deduped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.holder_name",
        "s.holder_joined_on",
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("approved_amount").alias("max_claim_approved_amount"))
)

with_counts = (
    with_details_holder.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.max_claim_approved_amount")
)

holder_id_stats = (
    with_counts
    .groupBy("holder_id")
    .agg(F.avg("max_claim_approved_amount").alias("avg_max_claim_approved_amount"))
)

above_average = (
    with_counts.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.max_claim_approved_amount") > F.col("g.avg_max_claim_approved_amount"))
    .select(
        "s.*",
        "g.avg_max_claim_approved_amount",
        F.round(F.col("s.max_claim_approved_amount") - F.col("g.avg_max_claim_approved_amount"), 2).alias("max_claim_approved_amount_minus_avg"),
    )
)

result = (
    above_average
    .filter(F.col("holder_joined_on") >= F.lit("2024-10-23").cast("date"))
    .select(
        "id",
        "holder_id",
        "end_date",
        "holder_joined_on",
        "holder_holder_name",
        "holder_joined_on_2",
        "max_claim_approved_amount",
        "avg_max_claim_approved_amount",
        "max_claim_approved_amount_minus_avg",
        F.date_format(F.col("holder_joined_on"), "yyyy-MM-dd").alias("holder_joined_on_label"),
        F.concat_ws(": ", F.col("holder_holder_name"), F.col("holder_name")).alias("holder_holder_name_holder_name_label"),
    )
    .orderBy(
        F.col("holder_joined_on_label").desc(),
        F.col("holder_holder_name").desc(),
        "id",
    )
    .limit(10)
)
