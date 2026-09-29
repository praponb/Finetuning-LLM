from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("premium") < 3719.7) | (F.col("start_date") < F.lit("2025-02-14").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        "s.premium_adj",
        "s.status_product_label",
        F.col("p.segment").alias("holder_segment"),
    )
)

screened = (
    joined.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
    .filter(F.col("holder_id") >= 10)
)

result = (
    screened
    .filter(F.col("start_date") < F.lit("2025-05-02").cast("date"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "premium_adj",
        "status_product_label",
        "holder_segment",
    )
    .orderBy(
        F.col("start_date").asc_nulls_last(),
        F.col("status_product_label").desc(),
        "id",
    )
    .limit(5)
)
