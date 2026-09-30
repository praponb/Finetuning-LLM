from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "location", "budget", "dept_name")
    .orderBy(F.col("dept_name").desc(), F.col("budget").desc())
)
