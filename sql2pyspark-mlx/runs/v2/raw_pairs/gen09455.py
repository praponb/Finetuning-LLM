from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("product_id").isin([16, 28]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy("id", F.col("quantity").desc())
)
