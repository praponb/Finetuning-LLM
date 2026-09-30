from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w_running = Window.partitionBy("level").orderBy("salary", "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L3"))) & (F.col("hire_date") >= F.lit("2023-08-24").cast("date")))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("hire_date_weekofyear", F.weekofyear("hire_date"))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "is_remote_label",
        "hire_date_weekofyear",
    )
)

with_windows = (
    latest
    .filter(F.col("full_name").like("O%"))
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        F.lag("salary").over(w_running).alias("prev_salary"),
        F.lag("hire_date_weekofyear").over(w_running).alias("prev_hire_date_weekofyear"),
        F.row_number().over(w_running).alias("seq_no"),
        F.first("job_title", ignorenulls=True).over(w3).alias("first_known_job_title"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.location"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("salary") >= 140000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.max("salary").alias("max_salary"),
        F.max(F.when(F.col("location") == "New York", F.col("salary"))).alias("new_york_max_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("prev_hire_date_weekofyear")).otherwise(0)).alias("director_prev_hire_date_weekofyear"),
    )
    .orderBy(F.col("new_york_max_salary").desc_nulls_first())
)
