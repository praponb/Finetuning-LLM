from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.row_number().over(w).alias("rn"),
        F.avg("unit_price").over(w2).alias("product_id_avg_unit_price"),
    )
)
