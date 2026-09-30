from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w2 = Window.orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w4 = Window.partitionBy("order_id", "product_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([8, 23, 29]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 50, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

result = (
    valid
    .select(
        "id",
        "unit_price_band",
        F.cume_dist().over(w).alias("unit_price_cume_dist"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_2"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4).alias("quantity_share"),
        F.rank().over(w5).alias("quantity_rank"),
    )
)
