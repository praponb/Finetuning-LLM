from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location", "dept_name")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.sum("budget").over(w2).alias("location_sum_budget"),
    )
)
