from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date_plus_86d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-3, -1)

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-11").cast("date"))
    .withColumn("clean_job_title", F.trim("job_title"))
    .withColumn("hire_date_plus_86d", F.date_add("hire_date", 86))
)

metrics = (
    filtered
    .select(
        "*",
        F.last("job_title", ignorenulls=True).over(w).alias("last_known_job_title"),
        F.avg("salary").over(w2).alias("salary_avg_3_prior"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.email").alias("employee_email"),
        F.col("b.full_name").alias("employee_full_name"),
    )
)

result = (
    joined
    .groupBy("level", "dept_id")
    .pivot("job_title", ["Director", "Designer"])
    .agg(F.sum("salary_avg_3_prior").alias("total"))
)
