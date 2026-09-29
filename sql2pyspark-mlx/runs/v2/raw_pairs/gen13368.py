from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "salary",
        "dept_id",
        "hire_date",
        F.row_number().over(w).alias("row_num"),
        F.percent_rank().over(w2).alias("hire_date_pct_rank"),
    )
)
