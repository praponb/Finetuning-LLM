from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.avg("unit_price").over(w).alias("product_id_avg_unit_price"),
    )
    .orderBy(F.col("product_id").desc())
)
