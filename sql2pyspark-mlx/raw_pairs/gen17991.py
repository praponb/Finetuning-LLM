from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]))
    .select(
        "id",
        "quantity",
        F.avg("quantity").over(w_part).alias("last4_avg_quantity"),
        F.max("quantity").over(w_part).alias("last4_max_quantity"),
        F.sum("quantity").over(w_part).alias("last4_sum_quantity"),
        F.count("quantity").over(w_part).alias("last4_count_quantity"),
    )
)
