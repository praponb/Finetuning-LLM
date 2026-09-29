from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select("id", "product_id", "units", "sale_date", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "product_id", "units", "sale_date")
    .orderBy(F.col("units").desc())
)
