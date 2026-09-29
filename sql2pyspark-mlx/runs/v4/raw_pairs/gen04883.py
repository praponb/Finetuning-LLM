from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.orderBy("rating", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("rating").between(2.3, 3.1))
    .select(
        "id",
        "rating",
        F.percent_rank().over(w).alias("rating_pct_rank"),
        F.round(F.avg("rating").over(w2), 2).alias("rating_moving_avg_7"),
    )
)
