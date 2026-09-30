from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 11]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.lead("unit_price", 1).over(w).alias("next_unit_price"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy(F.col("quantity").desc())
)
