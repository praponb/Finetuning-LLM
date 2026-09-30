from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price_rounded")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .withColumn("quantity_level", F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low"))
    .withColumn("unit_price_rounded", F.round("unit_price", 1))
)

result = (
    filtered
    .filter(F.col("unit_price_rounded") >= 30)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_level",
        F.dense_rank().over(w).alias("unit_price_rounded_dense_rank"),
        F.sum("unit_price_rounded").over(w2).alias("running_unit_price_rounded"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
        F.avg("unit_price_rounded").over(w4).alias("unit_price_rounded_avg_3"),
        F.sum("unit_price").over(w5).alias("running_unit_price"),
    )
)
