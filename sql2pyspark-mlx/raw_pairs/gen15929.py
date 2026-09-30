from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Books", "Grocery"]))
    .select("id", "category", "stock_qty", "cost", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "category", "stock_qty", "cost")
    .orderBy(F.col("id").desc(), F.col("cost").desc())
)
