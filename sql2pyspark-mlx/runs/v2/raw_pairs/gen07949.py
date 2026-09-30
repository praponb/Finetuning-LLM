from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.9)
    .select(
        "id",
        "city",
        "rating",
        F.count("*").over(w).alias("city_count"),
        F.first("city").over(w2).alias("first_city"),
    )
)
