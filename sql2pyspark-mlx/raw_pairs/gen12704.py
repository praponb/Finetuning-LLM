from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.row_number().over(w2).alias("seq_no"),
        F.dense_rank().over(w3).alias("unit_price_dense_rank"),
    )
)
