from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "region",
        "opened_date",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
        (F.col("sqft") - F.lag("sqft", 2).over(w2)).alias("sqft_change"),
    )
)
