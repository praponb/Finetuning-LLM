from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.avg("quantity").over(w_grp).alias("last7_avg_quantity"),
        F.max("quantity").over(w_grp).alias("last7_max_quantity"),
        F.min("quantity").over(w_grp).alias("last7_min_quantity"),
        F.count("quantity").over(w_grp).alias("last7_count_quantity"),
    )
)
