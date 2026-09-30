from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 3).alias("sqft_share"),
    )
)
