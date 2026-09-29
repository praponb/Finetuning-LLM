from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "noise-cancel headphones")
    .select("id", "supplier_id", "cost", "price", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "supplier_id", "cost", "price")
)
