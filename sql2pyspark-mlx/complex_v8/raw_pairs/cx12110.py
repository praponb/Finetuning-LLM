from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("status") == "EXPIRED") & (F.col("premium").between(1541.8, 3521.8)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

labeled = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "product_slug",
        "end_date_label",
        "premium_filled",
        F.month(F.col("end_date")).alias("end_date_month"),
        F.substring(F.trim(F.col("product_slug")), 1, 4).alias("product_slug_prefix"),
        (F.col("premium_filled") + F.col("premium")).alias("premium_filled_plus_premium"),
    )
)

enriched_holder = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") >= 0.2)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.product_slug",
        "s.end_date_label",
        "s.premium_filled",
        "s.end_date_month",
        "s.premium_filled_plus_premium",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    enriched_holder
    .select(
        "id",
        "product",
        "end_date",
        "end_date_label",
        "end_date_month",
        "premium_filled_plus_premium",
        "holder_name",
        "holder_risk_score",
    )
    .orderBy(F.col("holder_risk_score").desc(), "id")
    .limit(10)
)
