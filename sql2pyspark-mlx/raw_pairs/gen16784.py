from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("event_seq"),
    )
)
