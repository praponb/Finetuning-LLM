from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([16, 25]))
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.first("quantity").over(w).alias("first_quantity"),
    )
)
