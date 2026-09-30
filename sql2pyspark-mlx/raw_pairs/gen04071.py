from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy("years_since_hire_date", "id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-23").cast("date")) & F.col("manager_id").isNotNull())
    .withColumn("years_since_hire_date", (F.datediff(F.current_date(), "hire_date") / 365))
)

result = (
    base
    .withColumn("prev_years_since_hire_date", F.lag("years_since_hire_date").over(w))
    .withColumn("next_years_since_hire_date", F.lead("years_since_hire_date").over(w))
    .withColumn("prev_salary", F.lag("salary").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)
