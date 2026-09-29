from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.count("*").over(w2).alias("country_count"),
    )
)
