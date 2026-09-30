from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w_running = Window.orderBy("location_length", "id")
w3 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy(F.col("running_location_length").asc_nulls_last(), "id")
w6 = Window.partitionBy("budget_tier").orderBy(F.col("bangkok_count").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.length("location").alias("location_length"))
)

metrics = (
    base
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lag("location_length", 2, 0).over(w).alias("prev_location_length"),
        F.row_number().over(w_running).alias("seq_no"),
        F.lag("location_length", 1).over(w_running).alias("prev_location_length_2"),
        F.avg("location_length").over(w3).alias("location_length_avg_3_prior"),
        F.sum("location_length").over(w4).alias("running_location_length"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([18, 34, 37])), "left_anti")
)

kept_keys = (
    eligible
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("budget") <= 1309000)
        .select("id")
    )
)

verified = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("budget") >= 3930000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        "seq_no",
    )
    .agg(
        F.round(F.sum("prev_location_length_2") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("prev_location_length_2_per_marketing"),
        F.count("*").alias("row_count"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("seq_no_per_engineering"),
        F.sum(F.when(F.col("location") == "New York", F.col("prev_location_length_2")).otherwise(0)).alias("new_york_prev_location_length_2"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.round(F.avg("running_location_length"), 2).alias("avg_running_location_length"),
    )
    .withColumn("bangkok_count_percent_rank", F.percent_rank().over(w6))
)

result = (
    stats
    .select(
        "budget_tier",
        "seq_no",
        "prev_location_length_2_per_marketing",
        "row_count",
        "seq_no_per_engineering",
        "bangkok_count",
        "bangkok_count_percent_rank",
    )
)
