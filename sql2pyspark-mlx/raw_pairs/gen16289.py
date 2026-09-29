from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 3).alias("budget_share"),
    )
)
