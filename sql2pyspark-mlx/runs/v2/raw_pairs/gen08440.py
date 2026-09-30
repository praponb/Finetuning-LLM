from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.max("rating").over(w2).alias("city_max_rating"),
    )
)
