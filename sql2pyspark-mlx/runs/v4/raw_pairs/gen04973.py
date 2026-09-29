from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select("id", "country", "rating", F.lag("rating", 1).over(w).alias("prev_rating"))
    .orderBy("prev_rating", "rating")
)
