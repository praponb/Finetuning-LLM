from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.avg("unit_price").over(w).alias("order_id_avg_unit_price"),
    )
    .orderBy(F.col("order_id").desc(), F.col("id").desc())
)
