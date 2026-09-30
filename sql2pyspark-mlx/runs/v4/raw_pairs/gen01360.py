from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
    )
)
