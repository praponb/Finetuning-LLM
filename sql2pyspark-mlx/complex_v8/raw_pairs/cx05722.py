from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_group = Window.partitionBy("product_group")

scoped_policys = (
    df_policies
    .filter((F.col("holder_id") >= 9) & (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("product").isin("LIFE", "AUTO"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

labeled = (
    scoped_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "product_group",
        "days_since_end_date",
        F.round(F.col("days_since_end_date") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("days_since_end_date_per_premium"),
        F.when(F.col("product_group").isin("group_a", "group_b"), "group_a").when(F.col("product_group") == "other", "group_b").otherwise("other").alias("product_group_group"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

stats = (
    labeled
    .filter(F.col("product_group_group").isin("other", "group_b"))
    .groupBy(
        "product_group",
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "holder_id",
    )
    .agg(
        F.round(F.stddev("days_since_end_date_per_premium"), 2).alias("stddev_days_since_end_date_per_premium"),
        F.max("end_date").alias("last_end_date"),
        F.max("days_since_end_date_per_premium").alias("max_days_since_end_date_per_premium_check"),
    )
    .filter(F.col("max_days_since_end_date_per_premium_check") > 2.0)
    .drop("max_days_since_end_date_per_premium_check")
)

joined_holder = (
    stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") >= F.lit("2024-06-18").cast("date"))
    .select(
        "s.product_group",
        "s.end_date_month",
        "s.holder_id",
        "s.stddev_days_since_end_date_per_premium",
        "s.last_end_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    joined_holder
    .select(
        "product_group",
        "end_date_month",
        "holder_id",
        "stddev_days_since_end_date_per_premium",
        "last_end_date",
        "holder_joined_on",
        "holder_risk_score",
        F.size(F.collect_set("holder_id").over(w_product_group)).alias("distinct_holder_id_in_product_group"),
        F.round(F.avg("holder_risk_score").over(w_product_group), 2).alias("product_group_avg_holder_risk_score"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "HOME") & (F.col("p.status") == "CANCELLED"), "left_anti")
    .filter(F.col("holder_id") <= 15)
)

result = (
    qualified
    .select(
        "product_group",
        "holder_id",
        "stddev_days_since_end_date_per_premium",
        "last_end_date",
        "holder_risk_score",
        "distinct_holder_id_in_product_group",
        "product_group_avg_holder_risk_score",
    )
    .orderBy(
        F.col("holder_id").desc_nulls_first(),
        "product_group_avg_holder_risk_score",
    )
)
