from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .select(
        "id",
        "country",
        "rating",
        F.max("rating").over(w).alias("country_max_rating"),
        F.rank().over(w2).alias("rating_rank"),
    )
)
