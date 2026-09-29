from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("premium") < 3544.1)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

joined_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment") == "SME")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.days_start_date_to_end_date",
        "s.product_clean",
        "s.product_prefix",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

tagged = (
    joined_holder
    .select(
        "*",
        F.least(F.col("premium"), F.col("holder_risk_score")).alias("least_premium_holder_risk_score"),
        F.lower(F.regexp_replace(F.trim(F.col("product_clean")), " ", "_")).alias("product_clean_slug"),
    )
)

result = (
    tagged
    .groupBy("holder_id")
    .pivot("holder_segment", ["RETAIL", "SME"])
    .agg(F.min("premium"))
    .orderBy("holder_id")
)
