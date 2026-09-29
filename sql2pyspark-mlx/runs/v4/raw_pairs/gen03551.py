from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "*",
        F.lead("quantity", 2, 0).over(w_grp).alias("next_quantity"),
        F.lag("quantity", 2).over(w2).alias("prev_quantity"),
        F.ntile(5).over(w_grp).alias("quantity_bucket"),
    )
)
