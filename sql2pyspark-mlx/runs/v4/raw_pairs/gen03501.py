from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id")
w5 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

valid = (
    df_departments
    .filter(F.col("location") != "London")
    .select("*", F.trim("dept_name").alias("clean_dept_name"))
)

metrics = (
    valid
    .withColumn("budget_avg_5", F.avg("budget").over(w))
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w_ordered))
    .withColumn("location_sum_budget", F.sum("budget").over(w3))
    .withColumn("budget_bucket", F.ntile(3).over(w4))
    .withColumn("budget_bucket_2", F.ntile(3).over(w5))
)

matched_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_departments
        .filter(F.col("budget") > 1510000)
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.job_title").isin(["Intern", "Analyst", "Manager"])), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("budget_avg_5") >= 3957054, "LARGE").otherwise("SMALL").alias("budget_avg_5_tier"),
        F.when(F.col("budget_bucket") >= 8, "LARGE").otherwise("SMALL").alias("budget_bucket_tier"),
        "location",
    )
    .agg(
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.count("*").alias("cnt"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("cnt") > 1)
)
