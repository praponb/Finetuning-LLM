from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
