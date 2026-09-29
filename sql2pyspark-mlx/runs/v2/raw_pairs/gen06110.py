from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Grocery"]))
    .select("id", "stock_qty", F.ntile(2).over(w).alias("bucket"))
    .orderBy("bucket")
)
