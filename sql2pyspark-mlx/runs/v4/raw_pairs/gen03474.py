from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("cumulative_rating", F.sum("rating").over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w3))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w4))
    .withColumn("prev_rating", F.lag("rating", 1).over(w5))
)
