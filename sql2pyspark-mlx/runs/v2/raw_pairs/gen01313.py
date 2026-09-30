from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.length("product_name") >= 14)
    .select("id", "supplier_id", "cost", "product_name", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "supplier_id", "cost", "product_name")
    .orderBy("id")
)
