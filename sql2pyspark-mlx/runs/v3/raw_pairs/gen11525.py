from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "region",
        "city",
        "opened_date",
        F.first("region").over(w).alias("first_region"),
        F.row_number().over(w2).alias("opened_date_row"),
    )
)
