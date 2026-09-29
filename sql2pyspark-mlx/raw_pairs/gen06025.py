from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w2 = Window.partitionBy(F.date_trunc("week", "hire_date"), "manager_id").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy(F.date_trunc("month", "hire_date"), "dept_id").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("level", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("salary", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("dept_id").orderBy("salary_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("hire_date")).orderBy(F.col("running_salary").desc(), "id")
w9 = Window.partitionBy(F.month("hire_date"), "dept_id").orderBy("running_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("week_start").orderBy(F.col("num_job_titles").desc())

metrics = (
    df_employees
    .withColumn("next_salary", F.lead("salary", 1).over(w))
    .withColumn("salary_avg_5", F.avg("salary").over(w2))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w3))
    .withColumn("salary_avg_5_2", F.avg("salary").over(w4))
    .withColumn("running_salary", F.sum("salary").over(w5))
)

metrics_employees = (
    metrics
    .select(
        "*",
        F.avg("salary_avg_3_prior").over(w6).alias("salary_avg_3_prior_avg_5_prior"),
        F.sum("salary_avg_5_2").over(w7).alias("cumulative_salary_avg_5_2"),
        F.ntile(10).over(w8).alias("running_salary_bucket"),
        F.sum("salary_avg_3_prior").over(w9).alias("running_salary_avg_3_prior"),
    )
)

base = (
    metrics_employees
    .filter(F.col("full_name").like("Gra%"))
    .select(
        "*",
        F.round((F.col("salary_avg_5") * 1.15), 2).alias("adjusted_salary_avg_5"),
        F.round((F.col("running_salary_avg_3_prior") / F.col("cumulative_salary_avg_5_2")), 2).alias("running_salary_avg_3_prior_per_cumulative_salary_avg_5_2"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.hire_date",
        "a.is_remote",
        "a.next_salary",
        "a.salary_avg_5",
        "a.salary_avg_5_2",
        "a.running_salary",
        "a.cumulative_salary_avg_5_2",
        "a.running_salary_avg_3_prior",
        "a.running_salary_avg_3_prior_per_cumulative_salary_avg_5_2",
        F.col("b.hire_date").alias("employee_hire_date"),
        F.col("b.salary").alias("employee_salary"),
    )
)

stats = (
    joined
    .groupBy(
        F.date_trunc("WEEK", "employee_hire_date").alias("week_start"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        "dept_id",
    )
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.round(F.sum("cumulative_salary_avg_5_2") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("cumulative_salary_avg_5_2_per_engineer"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list_2"),
    )
    .withColumn("num_job_titles_rank", F.rank().over(w10))
)

result = (
    stats
    .select(
        "week_start",
        "year_start",
        "dept_id",
        "manager_events",
        "cumulative_salary_avg_5_2_per_engineer",
        "num_job_titles_rank",
    )
)
