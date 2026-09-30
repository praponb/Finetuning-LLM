from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .filter(F.col("salary") > 115000)
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        "dept_id",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.count("*").over(w2).alias("dept_id_count"),
    )
    .orderBy(F.col("next_salary").desc())
)
