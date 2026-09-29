from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Engineering", "Sales"]))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.rank().over(w2).alias("rnk"),
    )
)
