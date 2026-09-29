from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 12)
    .select("id", "country", "rating", F.lag("rating", 1).over(w).alias("prev_rating"))
)
