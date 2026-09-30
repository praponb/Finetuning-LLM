from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, -1)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.last("rating", ignorenulls=True).over(w).alias("last_known_rating"),
        F.avg("rating").over(w2).alias("rating_avg_3_prior"),
    )
)
