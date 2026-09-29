from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city", "country")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 3).alias("rating_share"),
    )
    .orderBy(F.col("rating_share").desc())
)
