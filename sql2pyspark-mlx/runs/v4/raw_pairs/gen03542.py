from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("days_since_last_restock", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-03-06").cast("date")) & (F.year("last_restock") == 2024))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
