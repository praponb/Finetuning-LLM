from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.dense_rank().over(w).alias("budget_dense_rank"),
    )
)
