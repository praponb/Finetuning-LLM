from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US"]))
    .select("id", "rating", F.lag("rating", 1).over(w).alias("prev_rating"))
)
