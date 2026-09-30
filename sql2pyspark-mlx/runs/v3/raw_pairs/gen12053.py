from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("quantity")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 3).alias("quantity_share"),
        F.percent_rank().over(w2).alias("quantity_pct_rank"),
    )
)
