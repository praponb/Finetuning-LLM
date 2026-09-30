from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .withColumn("min_rating_by_country", F.min("rating").over(w))
    .filter(F.col("rating") == F.col("min_rating_by_country"))
    .select("id", "country", "rating", "city")
    .orderBy(F.col("id").desc())
)
