from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")
w2 = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.avg("budget").over(w2).alias("location_avg_budget"),
    )
)
