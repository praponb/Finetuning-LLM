from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "order_id",
        "quantity",
        F.row_number().over(w).alias("rn"),
        F.percent_rank().over(w2).alias("quantity_pct_rank"),
    )
)
