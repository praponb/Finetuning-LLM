from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "store_id")

result = (
    df_inventory
    .filter(F.col("quantity") <= 239)
    .select(
        "id",
        "last_restock",
        "store_id",
        F.count("*").over(w).alias("last_restock_month_count"),
    )
)
