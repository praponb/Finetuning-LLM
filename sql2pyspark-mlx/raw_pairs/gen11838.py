from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("salary", "id")

with_windows = (
    df_employees
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        F.sum("salary").over(w).alias("running_sum_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
        F.count("salary").over(w).alias("running_count_salary"),
        F.lag("salary", 1).over(w2).alias("prev_salary"),
    )
)

result = (
    with_windows
    .withColumn("running_min_salary_tier", F.when(F.col("running_min_salary") >= 144000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_min_salary_tier", "manager_id"], ["running_min_salary_tier"], []],
        "running_min_salary_tier",
        "manager_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.avg("running_count_salary").alias("avg_running_count_salary"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)
