from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")

base = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
