from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_joined_on_runn = Window.orderBy(F.col("holder_joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_start_date_quarter_to = Window.partitionBy("status", F.to_date("start_date_quarter"))
w_all_by_end_date_ties = Window.orderBy("end_date")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("status").orderBy(F.col("end_date").desc(), F.col("id").desc())

scoped = (
    df_policies
    .filter(F.col("product") != "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
        F.when(F.col("premium") >= 3465.4, "large").when(F.col("premium") >= 2289.0, "medium").otherwise("small").alias("premium_tier"),
        F.date_trunc("year", F.col("end_date")).alias("end_date_year"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2033.8) & (F.col("p.start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-03-24").cast("date"))), "left_semi")
)

enriched = (
    qualified.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2024-12-21").cast("date"))
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "start_date_quarter",
        "premium_tier",
        "holder_name",
        "holder_joined_on",
        F.min("premium").over(w_all_by_holder_joined_on_runn).alias("min_premium_so_far"),
        F.size(F.collect_set("premium_tier").over(w_status_start_date_quarter_to)).alias("distinct_premium_tier_in_status_start_date_quart"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.last("holder_joined_on", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_holder_joined_on"),
    )
)

latest = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

rollup_base = (
    latest
    .groupBy(
        F.year("holder_joined_on").alias("holder_joined_on_year"),
        "distinct_premium_tier_in_status_start_date_quart",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("holder_name").isNotNull(), 1)) / F.count("*"), 1).alias("pct_holder_name_is_not_null"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.percentile_approx("min_premium_so_far", 0.5).alias("median_min_premium_so_far"),
        F.min("holder_joined_on").alias("first_holder_joined_on"),
        F.count("*").alias("row_count"),
        F.countDistinct("premium_tier").alias("distinct_premium_tiers"),
    )
)

tagged = (
    rollup_base
    .filter(F.col("median_premium") > 2120.0)
    .select(
        "holder_joined_on_year",
        "distinct_premium_tier_in_status_start_date_quart",
        "pct_holder_name_is_not_null",
        "median_premium",
        "row_count",
        "distinct_premium_tiers",
        F.abs(F.col("distinct_premium_tiers") - 6).alias("distinct_premium_tiers_distance"),
    )
)

result = (
    tagged
    .select(
        "holder_joined_on_year",
        "pct_holder_name_is_not_null",
        "median_premium",
        "row_count",
        "distinct_premium_tiers",
        "distinct_premium_tiers_distance",
        F.when(F.col("row_count") >= 8, "HIGH").when(F.col("row_count") >= 5, "MEDIUM").otherwise("LOW").alias("row_count_tier"),
    )
    .orderBy("distinct_premium_tiers_distance")
)
