from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-2, -1)
w_part = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("sqft").between(5719, 9728))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.avg("sqft").over(w_running).alias("sqft_avg_3_prior"),
        F.avg("sqft").over(w2).alias("sqft_avg_2_prior"),
        F.sum("sqft").over(w_part).alias("cumulative_sqft"),
    )
)
