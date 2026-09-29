from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("last_restock_minus_85d", "id")

cleaned = (
    df_inventory
    .filter(F.col("quantity") > 208)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_sub("last_restock", 85).alias("last_restock_minus_85d"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "product_id",
        "quantity",
        F.lag("quantity").over(w_grp).alias("prev_quantity"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)
