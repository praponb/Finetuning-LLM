from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-02-16")
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.rank().over(w).alias("rnk"),
        F.round(F.avg("sqft").over(w2), 2).alias("sqft_moving_avg_3"),
    )
)
