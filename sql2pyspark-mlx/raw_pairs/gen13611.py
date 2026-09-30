from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").between(150, 180))
    .withColumn("quantity_rounded", F.round("quantity", 2))
    .withColumn("adjusted_unit_price", F.round((F.col("unit_price") * 1.07), 2))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
