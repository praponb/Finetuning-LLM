from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-06-03", "2024-08-31"))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "product_id", "last_restock")
    .orderBy(F.col("product_id").desc(), "id")
)
