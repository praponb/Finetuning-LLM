from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-04-03"))) & (F.col("store_id") == 3))
    .select("*", F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "quantity", "last_restock_label")
)
