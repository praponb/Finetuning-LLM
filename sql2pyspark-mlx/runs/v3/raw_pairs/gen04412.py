from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "city",
        "opened_date",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
        F.round(F.avg("sqft").over(w2), 2).alias("sqft_moving_avg_3"),
    )
)
