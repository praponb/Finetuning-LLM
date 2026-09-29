from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 3).alias("quantity_share"),
        F.count("*").over(w2).alias("product_id_count"),
    )
)
