from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_segment = Window.partitionBy("status", "holder_segment")
w_all_by_premium_distance_full = Window.orderBy(F.col("premium_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter(F.col("product") == "AUTO")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.abs(F.col("premium") - 2111.3).alias("premium_distance"),
    )
)

with_parent_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on").between(F.lit("2024-05-23").cast("date"), F.lit("2025-07-03").cast("date"))), "left")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

enriched_holder = (
    with_parent_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.length(F.trim(F.col("p.holder_name"))) >= 8), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        "s.premium_distance",
        "s.holder_joined_on",
        "s.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.region").alias("holder_region"),
    )
)

enriched = (
    enriched_holder
    .filter(F.col("end_date") < F.lit("2026-08-31").cast("date"))
    .select(
        "*",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.lower(F.trim(F.col("holder_region"))).alias("holder_region_clean"),
        F.dayofweek(F.col("start_date")).alias("start_date_dayofweek"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_status_holder_segment), 2).alias("premium_vs_status_holder_segment_avg"),
        F.first("premium_filled").over(w_all_by_premium_distance_full).alias("first_premium_filled"),
        F.count(F.lit(1)).over(w_status_holder_segment).alias("rows_in_status_holder_segment"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region") == "Central"), "left_anti")
)

result = (
    filtered
    .filter(F.upper(F.trim(F.col("status_clean"))).like("E%"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "premium_filled",
        "premium_distance",
        "holder_joined_on",
        "holder_segment",
        "holder_holder_name",
        "holder_region",
        "status_clean",
        "start_date_dayofweek",
        "premium_vs_status_holder_segment_avg",
        "first_premium_filled",
        "rows_in_status_holder_segment",
        F.round(F.col("first_premium_filled") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("first_premium_filled_per_premium"),
        F.when(F.col("first_premium_filled") >= 3560.0, "large").when(F.col("first_premium_filled") >= 1880.0, "medium").otherwise("small").alias("first_premium_filled_tier"),
    )
    .orderBy("premium", "id")
    .limit(5)
)
