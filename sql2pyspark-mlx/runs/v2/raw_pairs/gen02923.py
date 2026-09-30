from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .select("id", "supplier_id", "cost", "category", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
)
