from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.max("budget").over(w).alias("location_max_budget"),
    )
    .orderBy(F.col("budget").desc())
)
