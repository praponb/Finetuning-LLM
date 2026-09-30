from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.concat_ws(" / ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
)

with_details_holder = (
    qualified.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.premium_filled",
        "s.product_status_label",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_details_holder
    .select(
        "id",
        "premium",
        "start_date",
        "premium_filled",
        "product_status_label",
        "holder_segment",
        "holder_joined_on",
    )
    .orderBy(F.col("premium").desc(), F.col("premium_filled").asc_nulls_last())
)
