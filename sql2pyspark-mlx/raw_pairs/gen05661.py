from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("location")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")

with_windows = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Finance"]))
    .withColumn("prev_budget", F.lag("budget").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

kept = (
    with_windows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.job_title").isin(["Intern", "Analyst"])), "left_anti")
)

enriched = (
    kept
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "prev_budget",
        F.sum("row_seq").over(w2).alias("running_row_seq"),
        F.sum("row_seq").over(w3).alias("cumulative_row_seq"),
        F.sum("prev_budget").over(w_hist).alias("location_sum_prev_budget"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title"))
)

deduped = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "dept_name",
        "prev_budget",
        "running_row_seq",
        "cumulative_row_seq",
        "location_sum_prev_budget",
        "level",
        "job_title",
    )
)

result = (
    deduped
    .groupBy(
        F.when(F.col("running_row_seq") >= 43, "LARGE").otherwise("SMALL").alias("running_row_seq_tier"),
        F.when(F.col("cumulative_row_seq") >= 41, "LARGE").otherwise("SMALL").alias("cumulative_row_seq_tier"),
        "level",
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("prev_budget")).otherwise(0)).alias("director_prev_budget"),
        F.sum(F.when(F.col("level") == "L5", F.col("running_row_seq"))).alias("l5_total_running_row_seq"),
    )
)
