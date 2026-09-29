from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price")

result = (
    df_products
    .filter(F.col("category") == "Home")
    .select("id", "brand", "price", F.percent_rank().over(w).alias("price_pct_rank"))
    .orderBy(F.col("price_pct_rank").desc())
)
