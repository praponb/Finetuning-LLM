from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        F.row_number().over(w).alias("rn"),
        F.count("*").over(w2).alias("product_id_count"),
    )
    .orderBy("id", F.col("last_restock").desc())
)
