from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc())
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "HR"]))
    .select(
        "id",
        "budget",
        "dept_name",
        F.rank().over(w).alias("rnk"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
