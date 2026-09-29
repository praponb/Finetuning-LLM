from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.lead("quantity").over(w_recent).alias("next_quantity"),
        F.lag("quantity").over(w_recent).alias("prev_quantity"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)
