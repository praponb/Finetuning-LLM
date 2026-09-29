from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .withColumn("avg_rating_by_country", F.avg("rating").over(w))
    .filter(F.col("rating") < F.col("avg_rating_by_country"))
    .select("id", "country", "rating", "city")
)
