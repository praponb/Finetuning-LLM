from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")

result = (
    df_departments
    .filter(F.col("budget") >= 1483000)
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 3).alias("budget_share"),
    )
)
