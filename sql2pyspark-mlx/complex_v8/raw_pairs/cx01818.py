from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_dayofweek_by_row_co = Window.partitionBy("hired_on_dayofweek").orderBy(F.col("row_count").desc(), "hired_on_dayofweek")
w_all_by_row_count_running = Window.orderBy(F.col("row_count").desc(), "hired_on_dayofweek").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_support_agents
    .filter((F.length(F.trim(F.col("agent_name"))) >= 7) & (F.col("tier").isin("T3", "T1")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.add_months(F.col("hired_on"), 3).alias("hired_on_plus_3m"),
        F.dayofweek(F.col("hired_on")).alias("hired_on_dayofweek"),
    )
)

stats = (
    prepared
    .filter(F.col("hired_on_plus_3m") < F.lit("2025-07-01").cast("date"))
    .groupBy("hired_on_dayofweek")
    .agg(
        F.round(F.stddev("hired_on_dayofweek"), 2).alias("stddev_hired_on_dayofweek"),
        F.count("*").alias("row_count"),
        F.min("hired_on").alias("first_hired_on"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.percentile_approx("hired_on_dayofweek", 0.5).alias("median_hired_on_dayofweek"),
    )
)

with_windows = (
    stats
    .select(
        "hired_on_dayofweek",
        "stddev_hired_on_dayofweek",
        "first_hired_on",
        "median_hired_on_dayofweek",
        F.ntile(10).over(w_hired_on_dayofweek_by_row_co).alias("row_count_decile"),
        F.count(F.lit(1)).over(w_all_by_row_count_running).alias("running_count"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("stddev_hired_on_dayofweek").alias("overall_avg_stddev_hired_on_dayofweek")))
    .filter(F.col("stddev_hired_on_dayofweek") > F.col("overall_avg_stddev_hired_on_dayofweek"))
    .drop("overall_avg_stddev_hired_on_dayofweek")
)

result = (
    with_activity
    .filter(F.col("stddev_hired_on_dayofweek") >= 1.1)
    .select(
        "hired_on_dayofweek",
        "stddev_hired_on_dayofweek",
        "first_hired_on",
        "median_hired_on_dayofweek",
        "row_count_decile",
    )
)
