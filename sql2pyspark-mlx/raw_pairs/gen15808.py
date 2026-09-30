from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("level", "manager_id")
w4 = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("salary").desc(), "id")
w6 = Window.orderBy("salary_avg_5", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("dept_id", "location").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location").orderBy("salary_avg_5", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("level").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "level",
        F.sum("salary").over(w).alias("running_salary"),
        F.avg("salary").over(w2).alias("salary_avg_3"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.avg("salary").over(w4).alias("salary_avg_5"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.dept_id",
        "a.level",
        "a.salary_avg_5",
        "a.event_seq",
        F.col("d.location"),
    )
)

metrics = (
    joined
    .filter(F.col("salary_avg_5") != 137646)
    .select(
        "id",
        "dept_id",
        "location",
        F.avg("salary_avg_5").over(w6).alias("salary_avg_5_avg_6_prior"),
        F.sum("event_seq").over(w7).alias("running_event_seq"),
        F.avg("salary_avg_5").over(w8).alias("salary_avg_5_avg_2_prior"),
        F.sum("salary_avg_5").over(w9).alias("cumulative_salary_avg_5"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.dept_name"))
)

with_dims_employees = (
    with_dims
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.location").alias("department_location"),
        F.col("b.budget"),
        F.col("b.dept_name").alias("department_dept_name"),
    )
)

joined_employees = (
    with_dims_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.location").alias("department_location_2"))
)

result = (
    joined_employees
    .withColumn("salary_avg_5_avg_6_prior_tier", F.when(F.col("salary_avg_5_avg_6_prior") >= 157080, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_event_seq_tier", F.when(F.col("running_event_seq") >= 38, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["department_dept_name", "salary_avg_5_avg_6_prior_tier", "running_event_seq_tier"], ["department_dept_name", "salary_avg_5_avg_6_prior_tier"]],
        "department_dept_name",
        "salary_avg_5_avg_6_prior_tier",
        "running_event_seq_tier",
    )
    .agg(
        F.sort_array(F.collect_set("department_location")).alias("department_location_list"),
        F.percentile_approx("cumulative_salary_avg_5", 0.5).alias("cumulative_salary_avg_5_p50"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("salary_avg_5_avg_2_prior")).otherwise(0)).alias("bangkok_salary_avg_5_avg_2_prior"),
        F.percentile_approx("cumulative_salary_avg_5", [0.25, 0.5, 0.75]).alias("cumulative_salary_avg_5_quartiles"),
        F.grouping("department_dept_name").alias("gid"),
    )
    .filter(F.col("cumulative_salary_avg_5_p50") > 0)
)
