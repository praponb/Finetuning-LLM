from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_last())
w4 = Window.partitionBy("supplier_id", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w2))
    .withColumn("cost_dense_rank", F.dense_rank().over(w3))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w4))
)
