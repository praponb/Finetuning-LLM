from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

result = (
    df_products
    .withColumn("last_known_brand", F.last("brand", ignorenulls=True).over(w))
    .withColumn("next_stock_qty", F.lead("stock_qty", 1).over(w2))
)
