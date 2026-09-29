from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("brand").orderBy("cost", "id")
w4 = Window.partitionBy("category", "brand")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("stock_qty").alias("max_product_stock_qty"))

enriched = (
    df_products
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.max("price").over(w2).alias("brand_max_price"),
        F.lag("price", 1).over(w3).alias("prev_price"),
        F.corr("stock_qty", "price").over(w4).alias("corr_stock_qty_price"),
    )
)

result = (
    enriched
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "cost_avg_6_prior",
        F.when(F.col("brand").rlike("^(Acme|Umbrella)$"), "EXTENDED").otherwise("GROUP_B").alias("brand_zone"),
        F.col("max_product_stock_qty"),
    )
    .orderBy("stock_qty", F.col("price").desc())
)
