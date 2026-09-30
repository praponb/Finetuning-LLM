from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("is_remote_flag_2").asc_nulls_last())
w_part = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-29").cast("date")) & (F.col("job_title").eqNullSafe(F.lit("Analyst"))))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.col("is_remote").cast("int").alias("is_remote_flag_2"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    deduped
    .filter(F.col("manager_id").isin([1, 37]))
    .select(
        "*",
        F.min("is_remote_flag").over(w_part).alias("last4_min_is_remote_flag"),
        F.count("is_remote_flag").over(w_part).alias("last4_count_is_remote_flag"),
        F.sum("is_remote_flag").over(w_part).alias("last4_sum_is_remote_flag"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.manager_id",
        "e.salary",
        "e.level",
        "e.is_remote_flag_2",
        "e.last4_min_is_remote_flag",
        "e.last4_count_is_remote_flag",
        F.col("p.job_title"),
        F.col("p.level").alias("employee_level"),
        F.col("p.salary").alias("employee_salary"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "level",
        "last4_min_is_remote_flag",
        "last4_count_is_remote_flag",
        "job_title",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
