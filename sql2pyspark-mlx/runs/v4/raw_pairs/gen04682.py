from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "city",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_7"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
    )
)
