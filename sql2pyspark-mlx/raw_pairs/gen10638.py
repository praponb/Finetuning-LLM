from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity_filled", "id")

valid = (
    df_inventory
    .filter(F.month("last_restock").isin([6, 9, 11]))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

result = (
    valid
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "quantity",
        "last_restock",
        F.min("quantity").over(w).alias("running_min_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
