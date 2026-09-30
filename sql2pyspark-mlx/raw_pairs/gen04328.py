from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_id").orderBy("salary_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.month("hire_date"), "dept_id").orderBy("salary_int_avg_3_prior", "id")
w_part = Window.partitionBy(F.month("hire_date"), "job_title").orderBy("salary_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("manager_id").orderBy("running_count_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("manager_id").orderBy("running_sum_last_known_salary_int", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("manager_id", "job_title")
w8 = Window.partitionBy("manager_id").orderBy(F.col("running_sum_last_known_salary_int").desc(), "id")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull() & (~F.col("job_title").eqNullSafe(F.lit("Designer"))))
    .select("*", F.col("salary").cast("int").alias("salary_int"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.count("salary").over(w_recent).alias("running_count_salary"),
        F.avg("salary").over(w_recent).alias("running_avg_salary"),
        F.sum("salary").over(w_recent).alias("running_sum_salary"),
        F.avg("salary_int").over(w2).alias("salary_int_avg_3_prior"),
    )
)

scored = (
    metrics
    .filter(F.col("hire_date") > "2024-05-07")
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "salary_int",
        "running_count_salary",
        "running_avg_salary",
        "running_sum_salary",
        F.lead("running_count_salary", 1).over(w3).alias("next_running_count_salary"),
        F.last("salary_int", ignorenulls=True).over(w_part).alias("last_known_salary_int"),
    )
)

scored_employees = (
    scored
    .select(
        "*",
        F.count("last_known_salary_int").over(w_hist).alias("running_count_last_known_salary_int"),
        F.avg("last_known_salary_int").over(w_hist).alias("running_avg_last_known_salary_int"),
        F.sum("last_known_salary_int").over(w_hist).alias("running_sum_last_known_salary_int"),
    )
)

metrics_employees = (
    scored_employees
    .select(
        "*",
        F.avg("next_running_count_salary").over(w6).alias("next_running_count_salary_avg_6"),
        F.sum("salary").over(w7).alias("manager_id_sum_salary"),
        F.ntile(4).over(w8).alias("running_sum_last_known_salary_int_bucket"),
    )
)

result = (
    metrics_employees
    .select(
        "id",
        "job_title",
        F.col("running_avg_last_known_salary_int").alias("RUNNING_AVG_LAST_KNOWN_SALARY_INT_VAL"),
        F.col("running_avg_salary").alias("RUNNING_AVG_SALARY"),
    )
    .unpivot(
        ids=["id", "job_title"],
        values=["RUNNING_AVG_LAST_KNOWN_SALARY_INT_VAL", "RUNNING_AVG_SALARY"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
