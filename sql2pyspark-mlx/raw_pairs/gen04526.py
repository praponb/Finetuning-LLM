from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("hire_date"), "level").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("job_title", "manager_id").orderBy("salary_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_employees
    .withColumn("running_count_salary", F.count("salary").over(w))
    .withColumn("running_max_salary", F.max("salary").over(w))
    .withColumn("running_sum_salary", F.sum("salary").over(w))
    .withColumn("salary_avg_5", F.avg("salary").over(w2))
)

metrics = (
    with_windows
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.sum("running_max_salary").over(w4).alias("running_running_max_salary"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.budget"), F.col("b.dept_name"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.salary",
        "a.hire_date",
        "a.is_remote",
        "a.level",
        "a.running_count_salary",
        "a.running_max_salary",
        "a.salary_avg_5",
        "a.event_seq",
        F.col("e.salary").alias("employee_salary"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
    )
)

totals = (
    joined
    .groupBy(
        F.when(F.col("running_max_salary") >= 148000, "PREMIUM").otherwise("STANDARD").alias("running_max_salary_tier"),
        F.when(F.col("salary") >= 145000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("employee_level") == "L5", 1)), F.lit(0)), 2).alias("event_seq_per_l5"),
        F.countDistinct("employee_level").alias("distinct_employee_levels"),
        F.count(F.when(F.col("employee_level") == "L5", 1)).alias("l5_rows"),
    )
)

result = (
    totals
    .select(
        "running_max_salary_tier",
        "salary_tier",
        "event_seq_per_l5",
        "distinct_employee_levels",
        "l5_rows",
    )
    .orderBy(F.col("distinct_employee_levels").asc_nulls_first())
)
