from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([21, 23, 39]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        "quantity",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lead("unit_price", 2).over(w2).alias("next_unit_price"),
    )
)
