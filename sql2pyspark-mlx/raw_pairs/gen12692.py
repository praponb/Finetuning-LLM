from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "dept_id",
        "salary",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("rnk"),
    )
)
