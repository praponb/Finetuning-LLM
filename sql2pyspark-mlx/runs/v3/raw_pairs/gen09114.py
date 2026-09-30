from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("price") <= 1140)
    .select("id", "supplier_id", "cost", "brand", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .orderBy("id")
)
