from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
        F.lag("rating", 1, 0).over(w).alias("prev_rating"),
    )
)
