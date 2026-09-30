from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        F.round(F.avg("rating").over(w), 2).alias("rating_moving_avg_4"),
    )
)
