from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Chiang Mai"]))
    .select(
        "id",
        "budget",
        "location",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.lag("budget", 2).over(w2).alias("prev_budget_2"),
    )
)
