from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([27, 28, 36]))
    .select(
        "id",
        "order_id",
        "quantity",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 3).alias("quantity_share"),
    )
    .orderBy(F.col("id").desc())
)
