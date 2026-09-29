from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "hire_date",
        "job_title",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
)
