from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date").desc(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.max("quantity").alias("max_order_item_quantity"))

latest = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "rank_in_group",
        F.col("max_order_item_quantity"),
    )
    .orderBy(F.col("store_id").desc_nulls_last())
)
