from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
)
