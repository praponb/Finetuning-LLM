from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.col("product_name").like("Ba%"))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .orderBy(F.col("category").desc())
)
