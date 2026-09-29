from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("src.*", F.col("c.city"), F.col("c.country"), F.col("c.segment"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.customer_id", "src.tag", F.col("p.name"), F.col("p.signup_date"))
)

base = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .withColumn("name_tag_key", F.concat_ws("-", "name", "tag"))
    .withColumn("signup_date_month_end", F.last_day("signup_date"))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("UNKNOWN")))
)

with_dims_carts = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.country"))
)

joined = (
    with_dims_carts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.is_active"))
    .select(
        "a.*",
        F.col("p.segment"),
        F.col("p.country").alias("customer_country"),
        F.col("p.age"),
    )
)

result = (
    joined
    .select(
        "tag",
        "name",
        "signup_date",
        "name_tag_key",
        "signup_date_month_end",
        "resolved_tag",
        "country",
        "segment",
        "customer_country",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "GROUP_A").otherwise("CORE").alias("segment_zone"),
    )
)
