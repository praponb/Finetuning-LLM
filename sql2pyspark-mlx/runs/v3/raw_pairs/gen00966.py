from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city", "country").orderBy("rating")

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "city",
        "country",
        F.row_number().over(w).alias("rn"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
