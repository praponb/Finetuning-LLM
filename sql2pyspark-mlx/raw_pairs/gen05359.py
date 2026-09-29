from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "city").orderBy("sqft")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "city",
        "sqft",
        "region",
        F.rank().over(w).alias("rnk"),
        F.lag("sqft", 1).over(w2).alias("prev_sqft"),
    )
)
