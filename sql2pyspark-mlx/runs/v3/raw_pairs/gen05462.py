from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("manager_id") == 16)
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        F.first("hire_date").over(w).alias("first_hire_date"),
    )
    .orderBy(F.col("dept_id").desc())
)
