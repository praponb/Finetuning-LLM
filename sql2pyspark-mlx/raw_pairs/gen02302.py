from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") == "HR"))
    .select("*", F.upper("dept_name").alias("dept_name_upper"))
)

result = (
    valid
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_upper",
        F.rank().over(w).alias("budget_rank"),
        F.ntile(10).over(w2).alias("budget_bucket"),
    )
)
