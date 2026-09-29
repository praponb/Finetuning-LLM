from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.orderBy("quantity")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
