from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "quantity", "unit_price")
    .orderBy(F.col("unit_price").desc(), F.col("order_id").desc())
)
