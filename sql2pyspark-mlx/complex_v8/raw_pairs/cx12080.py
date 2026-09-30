from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc_nulls_first(), "id")
w_all_by_hired_on_running = Window.orderBy(F.col("hired_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_agents = (
    df_support_agents
    .filter((F.col("tier") == "T1") & (F.col("team") == "VIP"))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
    )
)

with_trends = (
    filtered_agents
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.last("hired_on").over(w_all_by_hired_on_full).alias("last_hired_on"),
        F.lag("hired_on").over(w_all_by_hired_on).alias("prev_hired_on"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
        F.last("hired_on", ignorenulls=True).over(w_all_by_hired_on_running).alias("last_known_hired_on"),
        F.row_number().over(w_all_by_hired_on).alias("rn"),
    )
)

result = (
    with_trends
    .rollup("team", "rn", "tier")
    .agg(
        F.grouping("team").alias("is_total_team"),
        F.max("days_since_prev_hired_on").alias("max_days_since_prev_hired_on"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "rn",
        F.coalesce(F.col("tier"), F.lit("ALL")).alias("tier"),
        "is_total_team",
        "max_days_since_prev_hired_on",
        "cnt",
    )
)
