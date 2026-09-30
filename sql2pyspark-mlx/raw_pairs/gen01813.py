from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")
w2 = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.sum("rating").over(w).alias("city_sum_rating"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
    .orderBy("city_sum_rating")
)
