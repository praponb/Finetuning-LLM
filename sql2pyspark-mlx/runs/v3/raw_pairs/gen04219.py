from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "global supply")
    .select("id", "city", "rating", F.lag("rating", 1, 0).over(w).alias("prev_rating"))
)
