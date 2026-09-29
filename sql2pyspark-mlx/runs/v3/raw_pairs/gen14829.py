from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("unit_price") < 270)
    .select(
        "id",
        "product_id",
        "quantity",
        F.max("quantity").over(w).alias("product_id_max_quantity"),
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 3).alias("quantity_share"),
    )
)
