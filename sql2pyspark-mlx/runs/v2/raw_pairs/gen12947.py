from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 4, 6]))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "supplier_id", "stock_qty", "category")
)
