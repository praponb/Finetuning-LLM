from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.count("*").over(w).alias("city_count"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
    .orderBy(F.col("id").desc())
)
