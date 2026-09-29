from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price")

valid = (
    df_order_items
    .filter(F.col("order_id") == 31)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "product_id", "unit_price")
)
