from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("premium") > 1042.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
    )
)

combined_details_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.product_clean",
        "s.start_date_month_end",
        "s.start_date_quarter",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

labeled = (
    combined_details_holder
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "product_clean",
        "start_date_month_end",
        "start_date_quarter",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date_month_end")).alias("days_since_start_date_month_end"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.sum("premium").alias("total_policy_premium"),
        F.count(F.when(F.col("product") == "TRAVEL", 1)).alias("policy_product_count"),
        F.count("*").alias("policy_count"),
    )
)

profiled = (
    labeled.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.total_policy_premium",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

combined_details = (
    profiled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") < 0.8)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.start_date_month_end",
        "s.start_date_quarter",
        "s.days_since_start_date_month_end",
        "s.total_policy_premium",
        "s.policy_product_count",
        "s.policy_count",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

combined_details_holder_2 = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.start_date_month_end",
        "s.start_date_quarter",
        "s.days_since_start_date_month_end",
        "s.total_policy_premium",
        "s.policy_product_count",
        "s.holder_risk_score",
        "s.holder_segment",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score_2"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    combined_details_holder_2
    .filter(F.col("holder_segment") == "RETAIL")
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "start_date_month_end",
        "start_date_quarter",
        "days_since_start_date_month_end",
        "total_policy_premium",
        "policy_product_count",
        "holder_name",
        "holder_risk_score_2",
        "holder_region",
        F.add_months(F.col("start_date_month_end"), 12).alias("start_date_month_end_plus_12m"),
        F.when(F.col("start_date_quarter") >= 3, "A").when(F.col("start_date_quarter") >= 2, "B").otherwise("C").alias("start_date_quarter_tier"),
    )
    .orderBy(F.col("id").asc(), F.col("product").desc())
)
