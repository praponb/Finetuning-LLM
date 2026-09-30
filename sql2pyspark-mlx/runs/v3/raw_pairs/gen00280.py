from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        F.rank().over(w).alias("last_restock_rank"),
    )
)
