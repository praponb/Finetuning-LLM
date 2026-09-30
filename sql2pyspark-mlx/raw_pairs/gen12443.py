from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("job_title") != "Director")
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        "dept_id",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.rank().over(w2).alias("hire_date_rank"),
    )
)
