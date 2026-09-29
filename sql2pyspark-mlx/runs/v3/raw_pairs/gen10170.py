from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.lead("sqft", 1).over(w2).alias("next_sqft_2"),
    )
)
