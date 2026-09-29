from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.col("stock_qty") <= 343)
    .select("id", "supplier_id", F.count("*").over(w).alias("supplier_id_count"))
    .orderBy("supplier_id")
)
