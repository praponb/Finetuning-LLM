from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "holder_region").orderBy(F.col("holder_joined_on_3").desc(), F.col("id").desc())

cleaned = (
    df_policies
    .filter((F.col("status") == "CANCELLED") & (F.col("product") == "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
    )
)

with_details_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.start_date_label",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

joined = (
    with_details_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score").between(0.1, 0.8)), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.start_date_label",
        "s.holder_joined_on",
        F.col("p.joined_on").alias("holder_joined_on_2"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_details = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.start_date_label",
        "s.holder_joined_on",
        "s.holder_joined_on_2",
        "s.holder_region",
        "s.holder_risk_score",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on_3"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("end_date").alias("last_policy_end_date"),
        F.max("end_date").alias("last_policy_end_date_2"),
    )
)

with_stats = (
    with_details.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.last_policy_end_date", "p.last_policy_end_date_2")
)

unique_rows = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    unique_rows
    .select(
        "*",
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
    )
)

result = (
    scored
    .filter(F.col("start_date_label").isNotNull())
    .select(
        "id",
        "holder_id",
        "start_date",
        "start_date_label",
        "holder_joined_on",
        "holder_joined_on_2",
        "holder_region",
        "holder_name",
        "holder_segment",
        "holder_joined_on_3",
        "last_policy_end_date_2",
        "holder_segment_or_unknown",
        F.lower(F.regexp_replace(F.trim(F.col("start_date_label")), " ", "_")).alias("start_date_label_slug"),
    )
)
