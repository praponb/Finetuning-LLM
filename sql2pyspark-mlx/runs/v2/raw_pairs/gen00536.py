from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "dept_id", "salary", "hire_date")
    .orderBy(F.col("id").desc())
)
