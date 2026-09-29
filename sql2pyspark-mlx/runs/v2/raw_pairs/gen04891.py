from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("salary") >= 137000)
    .select(
        "id",
        "dept_id",
        "hire_date",
        F.percent_rank().over(w).alias("hire_date_pct_rank"),
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
