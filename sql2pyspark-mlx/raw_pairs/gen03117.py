from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("level_length").desc())
w2 = Window.partitionBy(F.month("hire_date"), "dept_id")
w3 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "manager_id")

cleaned = (
    df_employees
    .filter(F.col("salary").between(57000, 118000) & F.col("manager_id").isNotNull())
    .withColumn("level_length", F.length("level"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.email").alias("employee_email"),
    )
)

matched_keys = (
    joined
    .select("manager_id", "level")
    .subtract(
        df_employees
        .filter(F.col("email").like("%@gmail.com"))
        .select("manager_id", "level")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "level",
        "level_length",
        "employee_email",
        F.rank().over(w).alias("level_length_rank"),
        F.avg("salary").over(w2).alias("hire_date_month_avg_salary"),
        F.max("salary").over(w3).alias("hire_date_period_max_salary"),
    )
)

result = (
    metrics
    .select(
        "full_name",
        "dept_id",
        "job_title",
        "level",
        "level_length",
        "employee_email",
        "level_length_rank",
        "hire_date_period_max_salary",
        F.when(F.col("level").rlike("^(L4|L3)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("level_zone"),
    )
)
