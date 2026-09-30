from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .withColumn("max_rating_by_country", F.max("rating").over(w))
    .filter(F.col("rating") == F.col("max_rating_by_country"))
    .select("id", "country", "rating")
)
