from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 3, 32, 34]))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.rank().over(w).alias("rnk"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
    .orderBy(F.col("id").desc())
)
