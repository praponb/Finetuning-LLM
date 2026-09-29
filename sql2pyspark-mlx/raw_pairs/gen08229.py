from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.dense_rank().over(w).alias("unit_price_dense_rank"),
        F.row_number().over(w2).alias("unit_price_row"),
    )
)
