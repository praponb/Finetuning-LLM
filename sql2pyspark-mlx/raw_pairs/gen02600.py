from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.min("quantity").over(w_grp).alias("running_min_quantity"),
        F.avg("quantity").over(w_grp).alias("running_avg_quantity"),
        F.max("quantity").over(w_grp).alias("running_max_quantity"),
    )
)
