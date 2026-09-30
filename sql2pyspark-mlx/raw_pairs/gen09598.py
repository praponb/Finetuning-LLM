from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("least_quantity_unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price_adjusted", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("order_id") == 38)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 1.07), 2).alias("unit_price_adjusted"),
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
)

result = (
    base
    .filter(F.col("unit_price") < 330)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lead("least_quantity_unit_price").over(w).alias("next_least_quantity_unit_price"),
        F.lag("least_quantity_unit_price").over(w).alias("prev_least_quantity_unit_price"),
        F.row_number().over(w).alias("seq_no"),
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
    )
)
