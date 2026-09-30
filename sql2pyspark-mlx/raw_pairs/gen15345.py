from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-5, -1)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.lead("quantity", 2).over(w_grp).alias("next_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_5_prior"),
    )
)
