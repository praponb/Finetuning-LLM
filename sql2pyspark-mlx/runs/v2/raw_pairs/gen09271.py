from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Grocery"]))
    .select("id", "price", F.row_number().over(w).alias("rn"))
)
