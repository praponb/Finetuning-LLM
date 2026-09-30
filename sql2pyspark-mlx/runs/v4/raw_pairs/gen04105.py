from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("launch_date", "id")
w2 = Window.partitionBy("category", "brand").orderBy(F.col("price").desc_nulls_last())
w_running = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("cost").asc_nulls_last())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

scored = (
    df_products
    .withColumn("next_stock_qty", F.lead("stock_qty", 2).over(w_hist))
    .withColumn("price_rank", F.rank().over(w2))
    .withColumn("cumulative_price", F.sum("price").over(w_running))
    .withColumn("cost_dense_rank", F.dense_rank().over(w_part))
)

result = (
    scored
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "launch_date",
        "tags",
        "supplier_id",
        "next_stock_qty",
        "cumulative_price",
        F.when(F.col("product_name").rlike("^(Office Chair|Running Shoes)$"), "GROUP_B").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("product_name_zone"),
        F.col("max_product_price"),
    )
    .orderBy(F.col("cumulative_price").asc_nulls_last(), "brand")
)
