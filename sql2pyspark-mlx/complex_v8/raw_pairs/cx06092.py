from pyspark.sql import functions as F

recent = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.when(F.col("status").isin("CANCELLED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

joined_holder = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2025-05-01").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.status_group",
        "s.start_date_quarter",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

screened = (
    joined_holder.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
    .filter(F.col("status_group") == "group_b")
)

combined_details_holder = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.start_date_quarter",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    combined_details_holder
    .select(
        "id",
        "holder_id",
        "start_date_quarter",
        "holder_name",
        "holder_region",
    )
    .orderBy("holder_region", F.col("id").desc())
)
