from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "order_id", "quantity", "product_id")
)
