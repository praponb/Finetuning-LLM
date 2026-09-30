from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Berlin"]))
    .select(
        "id",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
    )
)
