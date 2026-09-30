from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_4"),
    )
    .orderBy(F.col("sqft_moving_avg_4").desc())
)
