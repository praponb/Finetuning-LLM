from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "hire_date"), "job_title").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("job_title_length").desc(), "id")

base = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-07-03"))) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

with_windows = (
    base
    .filter(F.col("job_title").isin(["Manager", "Director"]))
    .select(
        "*",
        F.lead("salary", 2, 0).over(w).alias("next_salary"),
        F.lag("salary", 2, 0).over(w2).alias("prev_salary"),
    )
)

filtered = (
    with_windows
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-06-09"))) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("email")).alias("email_title"),
        (F.col("next_salary") * F.col("salary")).alias("next_salary_times_salary"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.email",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.is_remote",
        "e.is_remote_label",
        "e.next_salary",
        "e.next_salary_times_salary",
        F.col("p.job_title").alias("employee_job_title"),
    )
)

valid = (
    joined
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "employee_job_title",
        F.length("job_title").alias("job_title_length"),
        F.coalesce("employee_job_title", F.lit("N/A")).alias("resolved_employee_job_title"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "job_title_length",
        F.when(F.col("job_title").rlike("^(Manager|Designer)$"), "CORE").otherwise("PRIORITY").alias("job_title_zone"),
    )
    .orderBy(F.col("job_title").desc_nulls_last(), "job_title_length")
)
