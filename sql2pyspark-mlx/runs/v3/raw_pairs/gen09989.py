from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.avg("unit_price").over(w).alias("product_id_avg_unit_price"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("unit_price").desc())
)
