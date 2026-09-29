from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "product_id", "quantity", "order_id")
    .orderBy(F.col("product_id").desc())
)
