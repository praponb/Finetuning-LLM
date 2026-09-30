from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .filter(F.size("tags") > 1)
    .select("id", "category", "launch_date", "brand", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "category", "launch_date", "brand")
)
