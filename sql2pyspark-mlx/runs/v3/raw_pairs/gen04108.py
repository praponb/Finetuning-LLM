from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select("id", "product_id", "store_id", "units", F.rank().over(w).alias("rnk"))
)
