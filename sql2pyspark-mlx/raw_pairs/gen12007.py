from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "city",
        F.row_number().over(w).alias("sqft_row"),
        F.count("*").over(w2).alias("city_count"),
    )
    .orderBy("city")
)
