from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select("id", "product_id", "units", "store_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("units").desc())
)
