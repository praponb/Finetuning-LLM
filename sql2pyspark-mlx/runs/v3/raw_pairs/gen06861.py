from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .filter(F.col("hire_date").between("2025-01-24", "2025-06-01"))
    .select(
        "id",
        "dept_id",
        "salary",
        F.max("salary").over(w).alias("dept_id_max_salary"),
    )
    .orderBy(F.col("salary").desc())
)
