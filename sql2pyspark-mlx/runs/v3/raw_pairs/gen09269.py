from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Sales", "Engineering"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
        F.round(F.avg("budget").over(w2), 2).alias("budget_moving_avg_7"),
    )
    .orderBy(F.col("budget_moving_avg_7").desc())
)
