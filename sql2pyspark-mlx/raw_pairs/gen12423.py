from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 26, 27]))
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "order_id", "unit_price", "product_id")
    .orderBy(F.col("id").desc())
)
