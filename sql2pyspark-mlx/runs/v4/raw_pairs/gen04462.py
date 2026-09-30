from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.sum("budget").over(w).alias("location_sum_budget"),
    )
    .orderBy("location_sum_budget")
)
