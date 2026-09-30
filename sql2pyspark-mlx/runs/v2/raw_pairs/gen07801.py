from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("budget").between(1567000, 3774000))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
        F.max("budget").over(w2).alias("location_max_budget"),
    )
)
