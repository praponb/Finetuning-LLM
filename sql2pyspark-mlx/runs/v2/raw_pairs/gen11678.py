from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "store_id", "quantity", "product_id")
    .orderBy(F.col("product_id").desc())
)
