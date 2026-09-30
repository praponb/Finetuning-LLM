from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w5 = Window.partitionBy("product_id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

enriched = (
    df_order_items
    .select(
        "*",
        F.ntile(3).over(w).alias("unit_price_bucket"),
        F.avg("quantity").over(w2).alias("quantity_avg_2_prior"),
        F.dense_rank().over(w3).alias("unit_price_dense_rank"),
        F.ntile(5).over(w4).alias("unit_price_bucket_2"),
        F.corr("quantity", "unit_price").over(w5).alias("corr_quantity_unit_price"),
    )
)

result = (
    enriched
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_dense_rank",
        "unit_price_bucket_2",
        "corr_quantity_unit_price",
        F.col("max_store_sale_revenue"),
    )
)
