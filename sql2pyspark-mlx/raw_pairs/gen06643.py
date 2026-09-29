from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.first("country").over(w).alias("first_country"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
    )
)
