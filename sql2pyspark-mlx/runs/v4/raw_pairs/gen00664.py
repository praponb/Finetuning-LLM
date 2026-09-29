from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")

result = (
    df_products
    .select("id", "brand", "cost", "supplier_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("supplier_id").desc())
)
