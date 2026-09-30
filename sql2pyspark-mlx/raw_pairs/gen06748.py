from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock", "id").rowsBetween(-5, -1)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

metrics = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 4, 8]))
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_5_prior"),
    )
)

result = (
    metrics
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock",
        "last_known_quantity",
        "quantity_avg_5_prior",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("id").desc())
)
