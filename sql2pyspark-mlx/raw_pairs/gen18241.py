from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

with_windows = (
    df_inventory
    .filter(F.col("quantity") >= 60)
    .select(
        "id",
        "product_id",
        "quantity",
        F.rank().over(w_ordered).alias("quantity_rank"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.lead("quantity", 1).over(w_grp).alias("next_quantity"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.row_number().over(w_ordered_2).alias("row_seq"),
    )
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "running_quantity",
        "next_quantity",
        "cumulative_quantity",
        "row_seq",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("sum_inventory_quantity").asc_nulls_first())
)
