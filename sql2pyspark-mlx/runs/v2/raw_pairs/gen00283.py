from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "city",
        "rating",
        F.round(F.avg("rating").over(w), 2).alias("rating_moving_avg_3"),
    )
)
