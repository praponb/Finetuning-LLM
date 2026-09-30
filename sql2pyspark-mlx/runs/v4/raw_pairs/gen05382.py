from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("hire_date").desc(), "id")
w6 = Window.partitionBy("dept_id").orderBy(F.col("employee_salary_avg_6_prior").desc_nulls_last(), "id")
w_running = Window.partitionBy("dept_id").orderBy("employee_salary", "id")
w_ordered = Window.orderBy("hire_date", "id").rowsBetween(-3, -1)
w9 = Window.orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy("salary_avg_3_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("dept_id").orderBy("salary", "id")
w12 = Window.partitionBy("job_title").orderBy("salary_avg_3_filled", "id")
w13 = Window.partitionBy("job_title").orderBy("seq_no", "id")
w_ordered_2 = Window.orderBy(F.col("salary").desc())

joined = (
    df_employees
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.salary").alias("employee_salary"))
)

scored = (
    joined
    .select(
        "*",
        F.last("job_title", ignorenulls=True).over(w).alias("last_known_job_title"),
        F.avg("salary").over(w2).alias("salary_avg_3"),
        F.avg("employee_salary").over(w3).alias("employee_salary_avg_6_prior"),
        F.last("job_title", ignorenulls=True).over(w4).alias("last_known_job_title_2"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

cleaned = (
    scored
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-08-27"))) & F.col("level").isNotNull() & F.col("seq_no").isNull())
    .select(
        "*",
        F.upper(F.trim("email")).alias("email_normalized"),
        F.round((F.col("salary_avg_3") / F.col("employee_salary")), 2).alias("salary_avg_3_per_employee_salary"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest
    .select(
        "*",
        F.lead("salary", 2).over(w_running).alias("next_salary"),
        F.avg("salary").over(w_ordered).alias("salary_avg_3_prior"),
        F.avg("salary_avg_3").over(w9).alias("salary_avg_3_avg_5"),
    )
)

cleaned_employees = (
    metrics
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-11-04"))) & F.col("job_title").isNotNull() & (F.col("employee_salary") <= 101000))
    .withColumn("job_title_level", F.concat("job_title", F.lit(" "), "level"))
)

valid = (
    cleaned_employees
    .filter(F.col("next_salary") > 122000)
    .select("*", F.coalesce("salary_avg_3", F.lit(0)).alias("salary_avg_3_filled"))
)

result = (
    valid
    .withColumn("first_known_employee_salary_avg_6_prior", F.first("employee_salary_avg_6_prior", ignorenulls=True).over(w10))
    .withColumn("prev_salary_avg_3_filled", F.lag("salary_avg_3_filled", 1).over(w11))
    .withColumn("prev_salary_avg_3_per_employee_salary", F.lag("salary_avg_3_per_employee_salary", 2).over(w12))
    .withColumn("seq_no_bucket", F.ntile(4).over(w13))
    .withColumn("salary_percent_rank", F.percent_rank().over(w_ordered_2))
)
