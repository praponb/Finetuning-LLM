from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "budget",
        "location",
        (F.col("budget") - F.lag("budget", 1).over(w)).alias("budget_change"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)
