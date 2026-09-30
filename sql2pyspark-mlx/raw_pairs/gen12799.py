from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-2, -1)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_2_prior"),
    )
)
