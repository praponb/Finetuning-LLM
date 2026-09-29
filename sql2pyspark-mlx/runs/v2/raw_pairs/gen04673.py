from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget") < 2588000)
    .select(
        "id",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
    )
)
