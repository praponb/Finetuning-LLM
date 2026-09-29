from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([2, 4, 29, 30]))
    .select(
        "id",
        "dept_id",
        "salary",
        F.sum("salary").over(w).alias("dept_id_sum_salary"),
    )
)
