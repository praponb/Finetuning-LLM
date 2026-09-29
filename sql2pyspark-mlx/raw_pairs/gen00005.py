from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.orderBy(F.col("budget").desc(), "id")
w_part = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("budget") <= 3719000)
    .withColumn("location_prefix", F.substring("location", 1, 4))
    .withColumn("resolved_location", F.coalesce("location", F.lit("DIRECT")))
)

metrics = (
    base
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_prefix",
        "resolved_location",
        F.percent_rank().over(w).alias("budget_percent_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("budget").over(w_part).alias("running_budget"),
    )
)

filtered = (
    metrics
    .filter(F.col("running_budget") < 17759334)
    .withColumn("location_lower", F.lower("location"))
    .withColumn("location_clean", F.coalesce("location", F.lit("DIRECT")))
)

valid = (
    filtered
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_prefix",
        "budget_percent_rank",
        "running_budget",
        "location_lower",
        "location_clean",
        F.least("running_budget", "budget").alias("least_running_budget_budget"),
        F.round("row_seq", 0).alias("row_seq_rounded"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("budget") >= 3325000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        F.when(F.col("running_budget") >= 16741969, "LARGE").otherwise("SMALL").alias("running_budget_tier"),
    )
    .agg(
        F.avg(F.when(F.col("dept_name") == "Engineering", F.col("running_budget"))).alias("engineering_avg_running_budget"),
        F.avg(F.when(F.col("location") == "Berlin", F.col("budget_percent_rank"))).alias("berlin_avg_budget_percent_rank"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
    )
    .filter(F.col("berlin_avg_budget_percent_rank") < 1000000)
)
