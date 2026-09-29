from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        "region",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft_2"),
    )
)
