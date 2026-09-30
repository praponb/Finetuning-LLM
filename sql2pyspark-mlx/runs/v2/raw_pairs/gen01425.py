from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        "sqft",
        F.row_number().over(w).alias("rn"),
        F.lag("sqft", 1, 0).over(w2).alias("prev_sqft"),
    )
)
