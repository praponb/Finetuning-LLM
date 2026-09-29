from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") > 3.1)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "country", "rating", "supplier_name")
)
