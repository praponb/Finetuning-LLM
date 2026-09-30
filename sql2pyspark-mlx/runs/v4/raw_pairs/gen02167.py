from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date_plus_70d").desc())

base = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.date_add("hire_date", 70).alias("hire_date_plus_70d"),
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled_2"),
    )
)

deduped = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.salary"),
        F.col("b.job_title").alias("employee_job_title"),
    )
)

joined_employees = (
    joined
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.salary").alias("employee_salary"),
        F.col("p.hire_date"),
    )
)

result = (
    joined_employees
    .select("id", "employee_job_title", "salary", "employee_salary")
    .unpivot(
        ["id", "employee_job_title"],
        ["salary", "employee_salary"],
        "metric",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
