from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft") != 8478)
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.lag("sqft", 2).over(w).alias("prev_sqft"),
    )
)
