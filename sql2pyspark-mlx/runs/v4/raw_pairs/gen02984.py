from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("category")
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id")
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .withColumn("category_max_price", F.max("price").over(w_part))
    .withColumn("next_stock_qty", F.lead("stock_qty", 1).over(w2))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w3))
)
