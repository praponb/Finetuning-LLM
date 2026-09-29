from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.when(F.col("capacity") >= 11506, "large").when(F.col("capacity") >= 11005, "medium").otherwise("small").alias("capacity_tier"),
        F.date_format(F.col("opened_on"), "yyyy-MM-dd").alias("opened_on_label"),
        F.add_months(F.col("opened_on"), 1).alias("opened_on_plus_1m"),
    )
)

per_group = (
    valid_warehouses
    .filter(~F.col("capacity_tier").isin("medium", "large"))
    .groupBy("city")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("capacity").alias("max_capacity"),
        F.countDistinct("capacity_tier").alias("distinct_capacity_tiers"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("B%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_wh_code_like_b"),
    )
)

tagged = (
    per_group
    .select(
        "city",
        "n_rows",
        "max_capacity",
        "distinct_capacity_tiers",
        "pct_upper_trim_wh_code_like_b",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

result = (
    tagged
    .select(
        "city",
        F.col("distinct_capacity_tiers").alias("m_distinct_capacity_tiers"),
        F.col("max_capacity").alias("max_capacity_v"),
        F.col("n_rows").alias("n_rows_v"),
    )
    .unpivot(
        ids=["city"],
        values=["m_distinct_capacity_tiers", "max_capacity_v", "n_rows_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
