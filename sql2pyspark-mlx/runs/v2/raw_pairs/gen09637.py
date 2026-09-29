from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.sum("sqft").over(w).alias("running_sqft"),
    )
)
