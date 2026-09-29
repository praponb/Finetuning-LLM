from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        F.lead("salary", 1, 0).over(w).alias("next_salary"),
    )
    .orderBy("hire_date", F.col("id").desc())
)
