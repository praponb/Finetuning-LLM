from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "hire_date")).orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id")
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary_rank").desc())

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-03-18").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.when(F.col("salary") >= 148000, "High").when(F.col("salary") >= 42000, "Medium").otherwise("Low").alias("salary_band"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "salary_band",
        "job_title_filled",
        F.first("job_title", ignorenulls=True).over(w).alias("first_known_job_title"),
        F.lag("salary", 2).over(w2).alias("prev_salary"),
        F.rank().over(w3).alias("salary_rank"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "salary",
        "hire_date",
        "salary_band",
        "job_title_filled",
        "first_known_job_title",
    )
)

valid = (
    deduped
    .filter(F.col("hire_date") <= "2024-10-01")
    .withColumn("hire_date_period", F.date_trunc("week", "hire_date"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("salary") >= 134000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("hire_date_period").alias("first_hire_date_period"),
    )
    .filter(F.col("salary_p50") >= 0)
)
