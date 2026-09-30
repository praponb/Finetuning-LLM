from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")

result = (
    df_departments
    .filter(F.col("budget") <= 1222000)
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.min("budget").over(w).alias("location_min_budget"),
    )
)
