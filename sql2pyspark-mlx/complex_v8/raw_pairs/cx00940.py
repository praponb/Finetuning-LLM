from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_ms_played = Window.orderBy(F.col("total_ms_played").asc_nulls_last(), "source", "device", "played_at_year_month")
w_first_played_at_year_to_date = Window.partitionBy(F.to_date("first_played_at_year"), "source")

recent = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 302279, "large").when(F.col("ms_played") >= 73625, "medium").otherwise("small").alias("ms_played_tier"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

grouped = (
    recent
    .filter(F.col("ms_played") >= 71476)
    .groupBy(
        "source",
        "device",
        F.date_trunc("month", "played_at_year").alias("played_at_year_month"),
    )
    .agg(
        F.sum(F.when(F.col("device").isNotNull(), F.col("ms_played"))).alias("ms_played_device_is_not_null"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.sum("ms_played").alias("total_ms_played"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.min("played_at_year").alias("first_played_at_year"),
    )
    .filter(F.col("row_count") >= 2)
)

qualified = (
    grouped
    .crossJoin(grouped.agg(F.max("total_ms_played").alias("overall_max_total_ms_played")))
    .filter(F.col("total_ms_played") < F.col("overall_max_total_ms_played"))
    .drop("overall_max_total_ms_played")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.min("total_ms_played").alias("overall_min_total_ms_played")))
    .filter(F.col("total_ms_played") > F.col("overall_min_total_ms_played"))
    .drop("overall_min_total_ms_played")
)

sequenced = (
    qualified_2
    .select(
        "*",
        F.lag("total_ms_played", 3).over(w_all_by_total_ms_played).alias("prev3_total_ms_played"),
        F.round(F.avg("distinct_listener_ids").over(w_first_played_at_year_to_date), 2).alias("first_played_at_year_to_date_source_avg_distinct"),
    )
)

result = (
    sequenced
    .select(
        "source",
        "device",
        "ms_played_device_is_not_null",
        "distinct_listener_ids",
        "row_count",
    )
    .unpivot(
        ids=["source", "device"],
        values=["ms_played_device_is_not_null", "distinct_listener_ids", "row_count"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
