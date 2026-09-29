from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("holder_id") >= 8) | (F.col("start_date") < F.lit("2025-03-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.concat_ws("-", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

combined_details_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region") == "North"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.product_or_unknown",
        "s.product_slug",
        "s.product_status_label",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

screened = (
    combined_details_holder
    .crossJoin(combined_details_holder.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

combined_details_holder_2 = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

result = (
    combined_details_holder_2
    .rollup("status", "holder_region")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.countDistinct("product").alias("distinct_products"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("holder_region").asc_nulls_last(),
    )
)
