from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(~F.col("location").isin(["London", "Bangkok", "Singapore"]))
    .select(
        "id",
        "location",
        "budget",
        F.sum("budget").over(w).alias("running_budget"),
        F.row_number().over(w2).alias("row_num"),
    )
)
