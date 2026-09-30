from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select("id", "supplier_id", "price", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "supplier_id", "price")
    .orderBy(F.col("supplier_id").desc())
)
