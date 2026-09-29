from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "city",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_4"),
        F.ntile(3).over(w2).alias("opened_date_ntile"),
    )
)
