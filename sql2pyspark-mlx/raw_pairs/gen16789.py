from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_suppliers
    .select("id", "city", "rating", F.max("rating").over(w).alias("city_max_rating"))
    .orderBy(F.col("id").desc())
)
