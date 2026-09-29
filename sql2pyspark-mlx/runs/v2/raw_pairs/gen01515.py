from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.min("budget").over(w).alias("dept_name_min_budget"),
    )
)
