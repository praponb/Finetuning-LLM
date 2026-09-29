from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary_int").desc_nulls_first())
w2 = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy("years_since_hire_date", "id")
w3 = Window.orderBy("years_since_hire_date", "id")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("level").eqNullSafe(F.lit("L1"))))
    .withColumn("salary_int", F.col("salary").cast("int"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("OTHER")))
)

filtered = (
    cleaned
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "salary_int",
        (F.datediff(F.current_date(), "hire_date") / 365).alias("years_since_hire_date"),
    )
)

filtered_employees = (
    filtered
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-02-09"))) & F.month("hire_date").isin([7, 8, 10]))
    .select(
        "*",
        (F.col("salary") / F.nullif("salary_int", F.lit(0))).alias("salary_per_salary_int"),
    )
)

with_windows = (
    filtered_employees
    .select(
        "id",
        "full_name",
        "email",
        "salary_int",
        "salary_per_salary_int",
        F.cume_dist().over(w).alias("salary_int_cume_dist"),
        F.lag("years_since_hire_date", 1).over(w2).alias("prev_years_since_hire_date"),
        F.ntile(10).over(w3).alias("years_since_hire_date_bucket"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_employees
        .filter(F.col("hire_date").between("2023-03-01", "2025-01-01"))
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "salary_int",
        "salary_per_salary_int",
        "salary_int_cume_dist",
        "prev_years_since_hire_date",
        "years_since_hire_date_bucket",
    )
)
