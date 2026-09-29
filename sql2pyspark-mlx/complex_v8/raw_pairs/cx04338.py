from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2026-01-24").cast("date")) | (F.col("holder_id").isNotNull()))
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

derived = (
    recent_policys
    .filter(F.col("product") != "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

joined_holder = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.upper(F.trim(F.col("p.holder_name"))).like("G%"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.product_ref",
        "s.status_clean",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

result = (
    joined_holder
    .rollup("holder_id", "status", "holder_segment")
    .agg(
        F.count(F.when(F.col("product") == "TRAVEL", 1)).alias("n_product_travel"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("holder_segment"), F.lit("ALL")).alias("holder_segment"),
        "n_product_travel",
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
