from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.rank().over(w).alias("rating_rank"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("city")
)
