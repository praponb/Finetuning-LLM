from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_status_not_in_act = Window.orderBy(F.col("pct_status_not_in_active_expired").desc(), "start_date_year")
w_start_date_year = Window.partitionBy("start_date_year")
w_status_list_item = Window.partitionBy("status_list_item")
w_pct_status_not_in_active_exp = Window.partitionBy("pct_status_not_in_active_expired_decile", "status_list_item").orderBy("pct_status_not_in_active_expired", "start_date_year", "status_list_item").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_year_share = Window.orderBy(F.col("start_date_year_share_of_start_date_year").desc(), "start_date_year", "status_list_item")

scoped = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "EXPIRED")) & (F.col("product").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

totals = (
    scoped
    .groupBy(F.year("start_date").alias("start_date_year"))
    .agg(
        F.round(100 * F.count(F.when(~F.col("status").isin("ACTIVE", "EXPIRED"), 1)) / F.count("*"), 1).alias("pct_status_not_in_active_expired"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
)

ranked = (
    totals
    .withColumn("pct_status_not_in_active_expired_decile", F.ntile(10).over(w_all_by_pct_status_not_in_act))
    .withColumn("start_date_year_share_of_start_date_year", F.round(F.col("start_date_year") / F.nullif(F.sum("start_date_year").over(w_start_date_year), F.lit(0)), 4))
)

flattened = (
    ranked
    .select(
        "start_date_year",
        "pct_status_not_in_active_expired",
        "pct_status_not_in_active_expired_decile",
        "start_date_year_share_of_start_date_year",
        F.explode("status_list").alias("status_list_item"),
    )
)

with_windows = (
    flattened
    .withColumn("status_list_item_avg_start_date_year", F.round(F.avg("start_date_year").over(w_status_list_item), 2))
    .withColumn("last_start_date_year", F.last("start_date_year").over(w_pct_status_not_in_active_exp))
)

enriched = (
    with_windows
    .filter(F.col("start_date_year") > 2024)
    .select(
        "*",
        F.lower(F.trim(F.col("status_list_item"))).alias("status_list_item_clean"),
        F.when(F.col("start_date_year") >= 2026, "large").when(F.col("start_date_year") >= 2024, "medium").otherwise("small").alias("start_date_year_tier"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.avg("last_start_date_year").over(w_status_list_item), 2).alias("status_list_item_avg_last_start_date_year"),
        F.round(F.col("start_date_year") / F.nullif(F.sum("start_date_year").over(w_status_list_item), F.lit(0)), 4).alias("start_date_year_share_of_status_list_item"),
        F.size(F.collect_set("start_date_year_tier").over(w_status_list_item)).alias("distinct_start_date_year_tier_in_status_list_ite"),
        F.round(F.col("start_date_year") - F.avg("start_date_year").over(w_status_list_item), 2).alias("start_date_year_vs_status_list_item_avg"),
        F.lead("start_date_year").over(w_all_by_start_date_year_share).alias("next_start_date_year"),
    )
)

result = (
    with_trends
    .select(
        "start_date_year",
        "status_list_item",
        "start_date_year_vs_status_list_item_avg",
        "pct_status_not_in_active_expired",
        "status_list_item_avg_start_date_year",
        "start_date_year_share_of_status_list_item",
    )
    .unpivot(
        ids=["start_date_year", "status_list_item"],
        values=["start_date_year_vs_status_list_item_avg", "pct_status_not_in_active_expired", "status_list_item_avg_start_date_year", "start_date_year_share_of_status_list_item"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
