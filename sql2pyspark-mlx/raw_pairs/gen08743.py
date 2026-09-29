from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

ranked = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 410, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["product_id", "unit_price_tier", "order_id"], ["product_id", "unit_price_tier"]],
        "product_id",
        "unit_price_tier",
        "order_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
)
