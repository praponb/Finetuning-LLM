from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "city",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
)
