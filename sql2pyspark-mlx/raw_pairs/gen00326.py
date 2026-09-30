from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("seq_no"),
    )
)
