from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.row_number().over(w).alias("rating_row"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("rating").desc())
)
