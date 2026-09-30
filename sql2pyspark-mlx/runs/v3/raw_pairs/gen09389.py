from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location", "dept_name")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.max("budget").over(w).alias("location_max_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 3).alias("budget_share"),
    )
)
