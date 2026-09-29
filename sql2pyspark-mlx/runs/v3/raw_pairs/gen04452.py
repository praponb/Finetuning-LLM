from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity") <= 4)
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy("quantity")
)
