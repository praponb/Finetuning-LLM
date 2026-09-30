from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("full_name").isNull())
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        "job_title",
        (F.col("salary") - F.lag("salary", 1).over(w)).alias("salary_change"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("dept_id").desc(), "hire_date")
)
