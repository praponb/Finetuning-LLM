from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("dept_id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-08").cast("date")) & F.col("level").isNotNull())
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
)

with_windows = (
    filtered
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "job_title_title",
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share_2"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.level").alias("employee_level"),
        F.col("p.salary").alias("employee_salary"),
        F.col("p.email"),
    )
)

kept_keys = (
    with_dims
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L5", "L2", "L4"]))
        .select("manager_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.level").alias("employee_level"),
        F.col("p.job_title").alias("job_title"),
        F.col("a.employee_level").alias("employee_employee_level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.salary_share_2")).alias("avg_salary_share_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
