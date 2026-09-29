from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isin([10, 15, 18, 31]))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.lead("unit_price", 1).over(w).alias("next_unit_price"),
    )
)
