from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("latest_rank", "id")

filtered = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai", "London"]))
    .select(
        "*",
        F.substring("location", 1, 2).alias("location_prefix"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("location_min_budget", F.min("budget").over(w2))
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

base = (
    metrics
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Berlin"))))
    .withColumn("location_dept_name_key", F.concat_ws("/", "location", "dept_name"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

result = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3963000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.min("event_seq").alias("min_event_seq"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_rows"),
    )
    .orderBy(F.col("min_event_seq").desc_nulls_last())
)
