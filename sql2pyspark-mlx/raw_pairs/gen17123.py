from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("hire_date"), "manager_id")
w_ordered = Window.partitionBy(F.month("hire_date"), "level")
w_part = Window.partitionBy("level").orderBy("salary", "id")
w5 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .filter(F.col("dept_id") == 18)
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w))
    .withColumn("hire_date_year_min_salary", F.min("salary").over(w2))
    .withColumn("hire_date_month_max_salary", F.max("salary").over(w_ordered))
    .withColumn("salary_bucket", F.ntile(4).over(w_part))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w5))
)

result = (
    enriched
    .select(
        "id",
        "email",
        "job_title",
        "hire_date",
        "hire_date_month_max_salary",
        "first_known_job_title",
    )
)
