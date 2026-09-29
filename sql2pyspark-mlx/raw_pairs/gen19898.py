from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").asc_nulls_first(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price").between(220, 290))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("unit_price", 1).alias("unit_price_rounded"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
