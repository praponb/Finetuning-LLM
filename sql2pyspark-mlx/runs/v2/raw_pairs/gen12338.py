from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .select(
        "id",
        "location",
        "budget",
        F.max("budget").over(w).alias("location_max_budget"),
    )
)
