from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

valid = (
    df_order_items
    .filter(F.col("order_id").isin([19, 29, 40]))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
    .withColumn("unit_price_level", F.when(F.col("unit_price") >= 300, "High").when(F.col("unit_price") >= 180, "Medium").otherwise("Low"))
)

result = (
    valid
    .select(
        "id",
        "order_id",
        "quantity_str",
        F.avg("unit_price").over(w).alias("running_avg_unit_price"),
        F.max("unit_price").over(w).alias("running_max_unit_price"),
        F.min("unit_price").over(w).alias("running_min_unit_price"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
