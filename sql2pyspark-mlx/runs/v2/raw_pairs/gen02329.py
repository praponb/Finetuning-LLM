from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.lead("unit_price", 1).over(w).alias("next_unit_price"),
    )
    .orderBy(F.col("unit_price").desc())
)
