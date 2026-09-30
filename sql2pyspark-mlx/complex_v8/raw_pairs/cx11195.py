from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("holder_id") <= 13)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.when(F.col("product").isin("HOME", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
)

labeled = (
    prepared
    .filter(F.col("product_len") <= 9)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "product_len",
        "product_group",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        (F.floor(F.col("product_len") / 2) * 2).alias("product_len_bucket"),
        F.concat(F.col("product_group"), F.lit("#"), F.col("id").cast("string")).alias("product_group_ref"),
    )
)

combined_details_holder = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.product_len",
        "s.product_prefix",
        "s.product_group_ref",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    combined_details_holder
    .select(
        "id",
        "end_date",
        "product_prefix",
        "holder_risk_score",
        "holder_region",
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
        F.lower(F.trim(F.col("holder_region"))).alias("holder_region_clean"),
    )
    .orderBy(
        F.col("holder_region").asc_nulls_last(),
        F.col("holder_region_clean").desc(),
    )
)
