from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.avg("budget").over(w).alias("location_avg_budget"),
        F.rank().over(w2).alias("budget_rank"),
    )
)
