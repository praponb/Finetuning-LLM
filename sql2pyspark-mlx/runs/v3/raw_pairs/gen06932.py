from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select("id", "supplier_id", "stock_qty", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("rn").desc())
)
