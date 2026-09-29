from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.row_number().over(w2).alias("rating_row"),
    )
)
