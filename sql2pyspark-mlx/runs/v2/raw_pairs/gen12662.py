from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.lead("sqft", 1).over(w).alias("next_sqft"),
        F.sum("sqft").over(w).alias("running_sqft"),
    )
)
