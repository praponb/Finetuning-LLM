from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        "region",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
    )
)
