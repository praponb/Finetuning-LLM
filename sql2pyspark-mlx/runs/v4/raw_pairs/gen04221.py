from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("rn").desc(), "id")

latest = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.brand"))
)

result = (
    joined
    .select("id", "order_id", "quantity", "unit_price", "rn", "dedup_rank", "brand")
    .orderBy("quantity")
)
