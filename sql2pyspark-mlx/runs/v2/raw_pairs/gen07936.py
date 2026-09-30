from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_3"),
    )
    .orderBy(F.col("id").desc(), "opened_date")
)
