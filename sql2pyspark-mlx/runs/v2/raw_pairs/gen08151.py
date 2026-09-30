from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget") <= 1837000)
    .select(
        "id",
        "location",
        "budget",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.ntile(3).over(w2).alias("budget_ntile"),
    )
    .orderBy(F.col("budget_dense_rank").desc())
)
