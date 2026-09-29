from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("unit_price") - F.col("quantity")).alias("line_total"))
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "quantity", "unit_price", "line_total")
)
