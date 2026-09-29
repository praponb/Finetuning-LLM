from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.max("quantity").alias("max_order_item_quantity"))

deduped = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "quantity")
)

result = (
    deduped
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("id", "product_id", F.col("max_order_item_quantity"))
    .orderBy(F.col("max_order_item_quantity").desc_nulls_first())
)
