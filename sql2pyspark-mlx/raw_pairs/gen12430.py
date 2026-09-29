from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "HR", "Marketing"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w).alias("budget_avg_2_prior"),
        F.max("budget").over(w2).alias("dept_name_max_budget"),
    )
)
