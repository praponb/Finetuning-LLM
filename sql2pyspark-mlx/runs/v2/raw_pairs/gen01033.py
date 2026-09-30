from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
)
