from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Berlin", "London"]))
    .select(
        "id",
        "location",
        "budget",
        F.lead("budget", 2).over(w).alias("next_budget"),
        F.row_number().over(w2).alias("rn"),
    )
)
