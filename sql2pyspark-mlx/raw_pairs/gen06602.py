from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location") == "London")
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.lead("budget", 1).over(w).alias("next_budget"),
        (F.col("budget") - F.lag("budget", 1).over(w2)).alias("budget_change"),
    )
)
