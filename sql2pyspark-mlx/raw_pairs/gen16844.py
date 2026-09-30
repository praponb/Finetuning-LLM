from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("dept_name", "location").orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("dept_name", "location").orderBy("prev_budget", "id")
w5 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w6 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc(), "id")

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level").isNotNull()), "left_anti")
)

enriched = (
    kept
    .filter(F.col("location").isin(["Tokyo", "Berlin"]))
    .select(
        "*",
        F.lag("budget").over(w).alias("prev_budget"),
        F.row_number().over(w).alias("event_seq"),
        F.count("budget").over(w2).alias("location_count_budget"),
    )
)

with_windows = (
    enriched
    .withColumn("last7_sum_location_count_budget", F.sum("location_count_budget").over(w3))
    .withColumn("last7_min_location_count_budget", F.min("location_count_budget").over(w3))
    .withColumn("last7_max_location_count_budget", F.max("location_count_budget").over(w3))
    .withColumn("last7_count_location_count_budget", F.count("location_count_budget").over(w3))
    .withColumn("prev_budget_bucket", F.ntile(5).over(w4))
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "dept_name",
        "location",
        "prev_budget",
        "event_seq",
        "location_count_budget",
        "last7_sum_location_count_budget",
        "last7_count_location_count_budget",
        "prev_budget_bucket",
    )
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy("location")
    .pivot("dept_name", ["Sales", "HR", "Marketing"])
    .agg(F.sum("prev_budget"))
)
