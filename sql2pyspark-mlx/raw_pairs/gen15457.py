from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_inventory
    .withColumn("avg_quantity_by_product", F.avg("quantity").over(w))
    .filter(F.col("quantity") < F.col("avg_quantity_by_product"))
    .select("id", "product_id", "quantity")
    .orderBy(F.col("product_id").desc())
)
