from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("opened_date") > "2025-05-01")
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.rank().over(w2).alias("rnk"),
    )
)
