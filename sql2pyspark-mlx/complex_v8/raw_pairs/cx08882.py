from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("start_date").alias("first_policy_start_date"))
)

with_counts = (
    valid_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_start_date")
)

enriched = (
    with_counts
    .filter(F.col("product_status_label").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "first_policy_start_date",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.datediff(F.col("first_policy_start_date"), F.col("end_date")).alias("days_end_date_to_first_policy_start_date"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "first_policy_start_date",
        "status_ref",
        F.lower(F.regexp_replace(F.trim(F.col("status_ref")), " ", "_")).alias("status_ref_slug"),
    )
    .orderBy(F.col("product").desc(), F.col("holder_id").asc_nulls_last())
)
