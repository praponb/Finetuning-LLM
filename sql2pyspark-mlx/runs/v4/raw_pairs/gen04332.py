from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("quantity").desc())
)
