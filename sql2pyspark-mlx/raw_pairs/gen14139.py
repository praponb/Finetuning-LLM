from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first())

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.ntile(3).over(w_part).alias("budget_bucket"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
    )
)
