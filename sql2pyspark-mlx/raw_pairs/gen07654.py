from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("country") != "US")
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.row_number().over(w).alias("row_num"),
        F.count("*").over(w2).alias("city_count"),
    )
)
