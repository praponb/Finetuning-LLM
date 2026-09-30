from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("category").orderBy("cost", "id")
w3 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

metrics = (
    df_products
    .withColumn("corr_stock_qty_price", F.corr("stock_qty", "price").over(w))
    .withColumn("next_stock_qty", F.lead("stock_qty", 2).over(w2))
    .withColumn("next_stock_qty_2", F.lead("stock_qty", 2).over(w3))
)

result = (
    metrics
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "corr_stock_qty_price",
        "next_stock_qty",
        "next_stock_qty_2",
        F.when(F.col("brand").rlike("^(Stark|Initech)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Globex)$"), "PRIORITY").otherwise("EXTENDED").alias("brand_zone"),
        F.col("min_product_cost"),
    )
)
