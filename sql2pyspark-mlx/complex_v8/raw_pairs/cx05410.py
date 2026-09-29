from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("premium") >= 2289.0) | (F.col("product").isin("LIFE", "HEALTH")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

per_group = (
    filtered_policys
    .filter(F.col("product") == "AUTO")
    .groupBy(
        "product",
        F.date_trunc("month", "start_date").alias("start_date_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status") == "LAPSED", 1)) / F.count("*"), 1).alias("pct_status_lapsed"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.count(F.when(F.col("product").isin("HEALTH", "HOME", "LIFE"), 1)).alias("n_product_in_health_home_life"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.count(F.when(~F.col("status").isin("EXPIRED", "ACTIVE"), 1)).alias("n_status_not_in_expired_active"),
    )
)

labeled = (
    per_group
    .filter(F.col("n_status_not_in_expired_active") > 2)
    .select(
        "*",
        F.abs(F.col("pct_status_lapsed") - 65.0).alias("pct_status_lapsed_distance"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

result = (
    labeled
    .select(
        "product",
        "start_date_month",
        "pct_status_lapsed",
        "n_status_not_in_expired_active",
        "product_or_unknown",
        "product_slug",
    )
    .orderBy(
        F.col("product_or_unknown").desc(),
        F.col("product_slug").desc(),
        "product",
        "start_date_month",
    )
    .limit(20)
)
