from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
amount_by_store_id = df_orders.groupBy("store_id").agg(F.max("amount").alias("max_order_amount"))

deduped = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(amount_by_store_id, on="store_id", how="left")
    .select("id", "store_id", F.col("max_order_amount"))
    .orderBy(F.col("store_id").desc_nulls_last())
)
