from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Legal"]))
    .select(
        "id",
        "dept_name",
        "budget",
        F.row_number().over(w).alias("rn"),
        F.sum("budget").over(w2).alias("dept_name_sum_budget"),
    )
)
