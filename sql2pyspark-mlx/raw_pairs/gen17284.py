from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w_running = Window.partitionBy("dept_id", "level").orderBy(F.col("employee_salary_int").desc_nulls_last())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("level").orderBy("salary_percent_rank")
w5 = Window.partitionBy("level").orderBy(F.col("employee_salary_int_cume_dist").desc())
w6 = Window.partitionBy("dept_id")
w7 = Window.partitionBy("level", "dept_id").orderBy(F.col("employee_salary_int_cume_dist").asc_nulls_first())

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (~F.col("job_title").eqNullSafe(F.lit("Manager"))))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.concat("full_name", F.lit(", "), "email").alias("full_name_email"),
    )
)

matched_keys = (
    valid
    .select("level", "id")
    .intersectAll(
        df_employees
        .filter(~F.col("job_title").isin(["Engineer", "Analyst"]))
        .select("level", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.email"), F.col("d.salary").alias("employee_salary"))
)

cleaned = (
    joined
    .filter(F.col("job_title").isNotNull())
    .select(
        "full_name",
        "dept_id",
        "salary",
        "level",
        "email",
        "employee_salary",
        F.col("employee_salary").cast("int").alias("employee_salary_int"),
    )
)

metrics = (
    cleaned
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "*",
        F.percent_rank().over(w).alias("salary_percent_rank"),
        F.cume_dist().over(w_running).alias("employee_salary_int_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.avg("salary").over(w3).alias("level_avg_salary"),
        F.cume_dist().over(w4).alias("salary_percent_rank_cume_dist"),
        F.cume_dist().over(w5).alias("employee_salary_int_cume_dist_cume_dist"),
        F.max("employee_salary_int").over(w6).alias("dept_id_max_employee_salary_int"),
        F.rank().over(w7).alias("employee_salary_int_cume_dist_rank"),
    )
)
