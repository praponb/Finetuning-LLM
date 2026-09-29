from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_prior_rows_7d_b = Window.partitionBy("distinct_ids", "prior_rows_7d").orderBy("row_count")
w_prior_rows_7d_by_distinct_id = Window.partitionBy("prior_rows_7d").orderBy("distinct_ids", "prior_rows_7d").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count_distinct_ids = Window.partitionBy("row_count", "distinct_ids")
w_all_by_prior_rows_7d_vs_row = Window.orderBy("prior_rows_7d_vs_row_count_distinct_ids_avg", "prior_rows_7d")
w_all_by_prior_rows_7d_vs_row_2 = Window.orderBy("prior_rows_7d_vs_row_count_distinct_ids_avg", "prior_rows_7d").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

rollup_base = (
    matched
    .groupBy("prior_rows_7d")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("prior_kwh_7d").alias("avg_prior_kwh_7d_check"),
    )
    .filter(F.col("avg_prior_kwh_7d_check") > 60.0)
    .drop("avg_prior_kwh_7d_check")
)

sequenced = (
    rollup_base
    .select(
        "prior_rows_7d",
        "row_count",
        F.round(F.cume_dist().over(w_distinct_ids_prior_rows_7d_b), 3).alias("row_count_cume_dist"),
        F.sum("row_count").over(w_prior_rows_7d_by_distinct_id).alias("running_row_count"),
        F.round(F.col("prior_rows_7d") - F.avg("prior_rows_7d").over(w_row_count_distinct_ids), 2).alias("prior_rows_7d_vs_row_count_distinct_ids_avg"),
    )
)

windowed = (
    sequenced
    .withColumn("prior_rows_7d_change", F.col("prior_rows_7d") - F.lag("prior_rows_7d").over(w_all_by_prior_rows_7d_vs_row))
    .withColumn("first_known_row_count", F.first("row_count", ignorenulls=True).over(w_all_by_prior_rows_7d_vs_row_2))
)

result = (
    windowed
    .select(
        "prior_rows_7d",
        F.col("row_count_cume_dist").alias("row_count_cume_dist_v"),
        F.col("prior_rows_7d_vs_row_count_distinct_ids_avg").alias("m_prior_rows_7d_vs_row_count_distinct_ids_avg"),
    )
    .unpivot(
        ids=["prior_rows_7d"],
        values=["row_count_cume_dist_v", "m_prior_rows_7d_vs_row_count_distinct_ids_avg"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
