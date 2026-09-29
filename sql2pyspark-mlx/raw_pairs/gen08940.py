from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy("days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("salary", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy(F.date_trunc("week", "hire_date"), "dept_id").orderBy("days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("salary_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-03-23")))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
    .withColumn("salary_whole", F.floor("salary"))
)

result = (
    filtered
    .select(
        "*",
        F.sum("days_since_hire_date").over(w).alias("running_days_since_hire_date"),
        F.last("days_since_hire_date", ignorenulls=True).over(w2).alias("last_known_days_since_hire_date"),
        F.avg("salary").over(w3).alias("salary_avg_5_prior"),
        F.first("job_title", ignorenulls=True).over(w4).alias("first_known_job_title"),
        F.sum("salary").over(w_recent).alias("cumulative_salary"),
    )
)
