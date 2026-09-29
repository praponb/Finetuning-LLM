from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date_month_end", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.col("hire_date") > F.to_date(F.lit("2023-06-16"))))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
    .withColumn("level_clean", F.coalesce("level", F.lit("N/A")))
)

result = (
    valid
    .select(
        "*",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.ntile(4).over(w2).alias("salary_bucket"),
    )
)
