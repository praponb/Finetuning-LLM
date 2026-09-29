from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-30").cast("date")) & (F.col("store_id") == 6))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

result = (
    cleaned
    .select(
        "id",
        "product_id",
        "quantity_filled",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.avg("quantity_filled").over(w2).alias("quantity_filled_avg_6_prior"),
        F.sum("quantity_filled").over(w3).alias("cumulative_quantity_filled"),
    )
)
