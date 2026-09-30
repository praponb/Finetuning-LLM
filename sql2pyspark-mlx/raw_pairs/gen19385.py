from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.year("last_restock"), "product_id")

cleaned = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

result = (
    cleaned
    .withColumn("last7_min_last_restock_day", F.min("last_restock_day").over(w_ordered))
    .withColumn("last7_max_last_restock_day", F.max("last_restock_day").over(w_ordered))
    .withColumn("last7_sum_last_restock_day", F.sum("last_restock_day").over(w_ordered))
    .withColumn("last7_count_last_restock_day", F.count("last_restock_day").over(w_ordered))
    .withColumn("last_restock_year_min_quantity_scaled", F.min("quantity_scaled").over(w2))
)
