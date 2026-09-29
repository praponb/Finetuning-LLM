from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.row_number().over(w).alias("row_num"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
)
