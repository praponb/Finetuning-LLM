from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    with_windows
    .select("id", "prev_quantity", "next_quantity", "seq_no")
    .orderBy(F.col("id").desc_nulls_first())
    .limit(20)
)
