from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("budget").between(1142000, 1681000))
    .select(
        "id",
        "dept_name",
        "location",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.cume_dist().over(w2).alias("budget_cume_dist"),
    )
)
