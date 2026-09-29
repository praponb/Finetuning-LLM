from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc())

result = (
    df_products
    .select("id", "brand", "cost", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "brand", "cost")
)
