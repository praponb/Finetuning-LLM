from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_year_region_by_ris = Window.partitionBy(F.year("joined_on"), "region").orderBy(F.col("risk_score").desc_nulls_first(), "id")
w_joined_on_month = Window.partitionBy(F.date_trunc("month", "joined_on"))
w_joined_on_year_region_by_ris_2 = Window.partitionBy(F.year("joined_on"), "region").orderBy(F.col("risk_score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_by_days_since_joined = Window.partitionBy("region").orderBy(F.col("days_since_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_by_days_since_joined_2 = Window.partitionBy("region").orderBy(F.col("days_since_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_holders = (
    df_policyholders
    .filter((F.col("risk_score") >= 0.2) | (F.col("segment") == "SME"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

metrics = (
    base_holders
    .select(
        "*",
        F.lag("risk_score").over(w_joined_on_year_region_by_ris).alias("prev_risk_score"),
        F.count(F.lit(1)).over(w_joined_on_month).alias("rows_in_joined_on_month"),
        F.min("days_since_joined_on").over(w_joined_on_month).alias("joined_on_month_min_days_since_joined_on"),
        F.last("days_since_joined_on", ignorenulls=True).over(w_joined_on_year_region_by_ris_2).alias("last_known_days_since_joined_on"),
        F.size(F.collect_set("region").over(w_joined_on_month)).alias("distinct_region_in_joined_on_month"),
    )
)

with_windows = (
    metrics
    .withColumn("second_rows_in_joined_on_month", F.nth_value("rows_in_joined_on_month", 2).over(w_region_by_days_since_joined))
    .withColumn("last_known_days_since_joined_on_2", F.last("days_since_joined_on", ignorenulls=True).over(w_region_by_days_since_joined_2))
)

result = (
    with_windows
    .groupingSets(
        [["region", "distinct_region_in_joined_on_month"], ["region"]],
        "region", "distinct_region_in_joined_on_month",
    )
    .agg(
        F.max("second_rows_in_joined_on_month").alias("max_second_rows_in_joined_on_month"),
        F.round(F.stddev("second_rows_in_joined_on_month"), 2).alias("stddev_second_rows_in_joined_on_month"),
        F.min("days_since_joined_on").alias("min_days_since_joined_on"),
    )
    .select(
        F.coalesce(F.col("region"), F.lit("ALL")).alias("region"),
        "distinct_region_in_joined_on_month",
        "max_second_rows_in_joined_on_month",
        "stddev_second_rows_in_joined_on_month",
        "min_days_since_joined_on",
    )
)
