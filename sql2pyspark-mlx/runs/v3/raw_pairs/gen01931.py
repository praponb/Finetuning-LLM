from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w).alias("dept_name_avg_budget"),
    )
)
