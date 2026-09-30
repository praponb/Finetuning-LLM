from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")

result = (
    df_products
    .select("id", "brand", "cost", "category", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
)
