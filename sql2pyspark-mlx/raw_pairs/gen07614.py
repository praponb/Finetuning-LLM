from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") < 56000)
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        F.lead("salary", 2).over(w).alias("next_salary"),
    )
)
