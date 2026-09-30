from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date")).orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("id", "launch_date", "stock_qty", F.rank().over(w).alias("rnk"))
)
