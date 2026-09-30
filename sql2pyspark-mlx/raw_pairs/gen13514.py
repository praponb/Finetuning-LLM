from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 3).alias("budget_share"),
    )
    .orderBy("budget", "location")
)
