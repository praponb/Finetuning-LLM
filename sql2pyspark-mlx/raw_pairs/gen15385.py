from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock_plus_52d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock_plus_52d", "id").rowsBetween(-2, -1)

filtered = (
    df_inventory
    .filter(F.col("last_restock") > "2025-03-04")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 52).alias("last_restock_plus_52d"),
        F.date_sub("last_restock", 35).alias("last_restock_minus_35d"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_2_prior"),
    )
)
