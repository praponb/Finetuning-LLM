from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.row_number().over(w2).alias("row_num"),
    )
)
