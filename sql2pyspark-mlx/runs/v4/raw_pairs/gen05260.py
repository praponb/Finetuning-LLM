from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_first())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

with_windows = (
    df_store_sales
    .select(
        "*",
        F.lag("revenue", 1).over(w).alias("prev_revenue"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
        F.dense_rank().over(w3).alias("units_dense_rank"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "prev_revenue",
        "last_known_units",
        F.col("max_inventory_quantity"),
    )
    .orderBy("id")
)
