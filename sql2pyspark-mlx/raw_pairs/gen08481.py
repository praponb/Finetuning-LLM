from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 28]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
    )
)
