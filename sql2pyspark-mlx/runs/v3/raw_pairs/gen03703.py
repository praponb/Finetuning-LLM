from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.max("budget").over(w).alias("location_max_budget"),
    )
)
