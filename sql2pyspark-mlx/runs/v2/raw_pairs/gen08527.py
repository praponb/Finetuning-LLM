from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "dept_id",
        F.row_number().over(w).alias("rn"),
        F.ntile(4).over(w2).alias("bucket"),
    )
    .orderBy(F.col("rn").desc())
)
