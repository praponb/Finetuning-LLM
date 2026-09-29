from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "k",
        "value",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

valid = (
    flattened
    .filter(F.col("k").isin(["utm_source", "device"]))
    .select(
        "id",
        "customer_id",
        "k",
        "value",
        "tag",
        F.concat_ws("-", "k", "tag").alias("k_tag_key"),
        F.coalesce("tag", F.lit("OTHER")).alias("resolved_tag"),
    )
)

cleaned = (
    valid
    .filter(F.col("tag").isNotNull())
    .select(
        "id",
        "customer_id",
        "k",
        "tag",
        "k_tag_key",
        "resolved_tag",
        F.concat("k", F.lit(" "), "tag").alias("k_tag"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("resolved_tag_2"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.country"), F.col("p.city"))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "k",
        "tag",
        "resolved_tag",
        "k_tag",
        "resolved_tag_2",
        "country",
        "city",
        F.when(F.col("country").rlike("^(DE|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("country_zone"),
    )
)
