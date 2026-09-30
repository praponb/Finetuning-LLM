from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.sum("sqft").over(w2).alias("city_sum_sqft"),
    )
)
