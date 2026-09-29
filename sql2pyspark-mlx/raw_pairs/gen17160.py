from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "manager_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("manager_id").desc(), F.col("salary").desc())
)
