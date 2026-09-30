from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id")
w3 = Window.orderBy("quantity", "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.row_number().over(w).alias("row_seq"),
        F.max("days_since_last_restock").over(w2).alias("store_id_max_days_since_last_restock"),
        F.ntile(4).over(w3).alias("quantity_bucket"),
    )
)
