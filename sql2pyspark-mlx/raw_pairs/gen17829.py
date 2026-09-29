from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([8, 20, 39]))
    .select(
        "id",
        "dept_id",
        "salary",
        F.min("salary").over(w).alias("dept_id_min_salary"),
    )
    .orderBy(F.col("id").desc())
)
