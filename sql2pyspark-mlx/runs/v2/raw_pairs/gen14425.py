from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.rank().over(w).alias("budget_rank"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 3).alias("budget_share"),
    )
)
