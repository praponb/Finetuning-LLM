from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.sum("budget").over(w).alias("dept_name_sum_budget"),
        F.count("*").over(w2).alias("location_count"),
    )
    .orderBy("dept_name_sum_budget")
)
