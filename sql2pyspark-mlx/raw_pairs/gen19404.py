from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("budget_tier").orderBy(F.col("avg_seq_no").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Sales", "Finance"]), "Core").otherwise("Other"))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.is_remote")), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged_departments = (
    deduped
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isNull()), "left_anti")
)

eligible = (
    not_flagged_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") <= "2023-03-01"), "left_anti")
)

latest = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .withColumn("prev_budget", F.lag("budget").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget") >= 4487000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.avg(F.when(F.col("dept_name") == "HR", F.col("seq_no"))).alias("hr_avg_seq_no"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows_2"),
        F.count_if(F.col("dept_name") == "Marketing").alias("marketing_events"),
    )
    .filter(F.col("hr_avg_seq_no") < 1000000)
    .withColumn("avg_seq_no_percent_rank", F.percent_rank().over(w4))
    .orderBy("hr_rows")
)
