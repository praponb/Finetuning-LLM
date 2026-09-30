from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(-5, -1)
w_running = Window.orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.avg("sqft").over(w_part).alias("sqft_avg_5_prior"),
        F.lag("sqft", 1).over(w_running).alias("prev_sqft"),
    )
)
