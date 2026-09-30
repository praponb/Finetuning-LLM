from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 18]))
    .select(
        "id",
        "order_id",
        "unit_price",
        F.rank().over(w).alias("rnk"),
        F.lag("unit_price", 1).over(w2).alias("prev_unit_price"),
    )
)
