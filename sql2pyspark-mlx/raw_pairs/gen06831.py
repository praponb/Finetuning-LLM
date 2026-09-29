from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"))

result = (
    df_stores
    .filter(F.col("store_name").like("%Old%"))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.round(F.avg("sqft").over(w), 2).alias("sqft_moving_avg_7"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 3).alias("sqft_share"),
    )
)
