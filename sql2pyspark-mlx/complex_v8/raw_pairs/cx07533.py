from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_holder_joined_on = Window.partitionBy("status").orderBy("holder_joined_on", "id")
w_status_by_holder_joined_on_r = Window.partitionBy("status").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((F.col("product") == "TRAVEL") & (F.col("premium") > 1042.5))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        "holder_joined_on",
        F.lag("holder_joined_on").over(w_status_by_holder_joined_on).alias("prev_holder_joined_on"),
        F.first("end_date", ignorenulls=True).over(w_status_by_holder_joined_on_r).alias("first_known_end_date"),
    )
)

with_parent_holder = (
    metrics.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.holder_joined_on",
        "s.first_known_end_date",
        F.col("p.region").alias("holder_region"),
    )
)

combined_details = (
    with_parent_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.holder_joined_on",
        "s.holder_region",
        F.col("p.region").alias("holder_region_2"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

status_stats = (
    combined_details
    .groupBy("status")
    .agg(F.max("holder_risk_score").alias("max_holder_risk_score"))
)

vs_group = (
    combined_details.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.holder_risk_score") == F.col("g.max_holder_risk_score"))
    .select(
        "s.*",
        "g.max_holder_risk_score",
        F.round(F.col("s.holder_risk_score") - F.col("g.max_holder_risk_score"), 2).alias("holder_risk_score_minus_max"),
    )
)

rollup_base = (
    vs_group
    .groupBy("holder_id", "status")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.array_sort(F.collect_set("holder_segment")).alias("holder_segment_list"),
        F.round(100 * F.count(F.when(F.col("holder_region").isNotNull(), 1)) / F.count("*"), 1).alias("pct_holder_region_is_not_null"),
    )
)

result = (
    rollup_base
    .select(
        "holder_id",
        "status",
        "status_list",
        "pct_holder_region_is_not_null",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy(F.col("status").desc())
)
