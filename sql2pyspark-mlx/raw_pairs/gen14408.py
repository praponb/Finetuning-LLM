from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-08-09").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        F.when(F.col("quantity") >= 200, "High").when(F.col("quantity") >= 82, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

result = (
    base
    .select(
        "id",
        "quantity",
        F.sum("quantity").over(w_hist).alias("last7_sum_quantity"),
        F.max("quantity").over(w_hist).alias("last7_max_quantity"),
        F.min("quantity").over(w_hist).alias("last7_min_quantity"),
        F.count("quantity").over(w_hist).alias("last7_count_quantity"),
        F.ntile(5).over(w2).alias("quantity_bucket"),
    )
)
