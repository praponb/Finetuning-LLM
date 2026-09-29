from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "supplier_id", "cost")
)
