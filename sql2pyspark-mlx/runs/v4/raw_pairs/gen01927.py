from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "country", "rating", "city")
)
