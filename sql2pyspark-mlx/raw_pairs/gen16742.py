from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.col("brand").isNull())
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cumulative_price", F.sum("price").over(w2))
)
