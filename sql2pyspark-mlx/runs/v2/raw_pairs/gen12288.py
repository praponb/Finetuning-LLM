from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Li%"))
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "country", "rating")
    .orderBy(F.col("country").desc(), F.col("id").desc())
)
