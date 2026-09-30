from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("stock_qty").alias("avg_product_stock_qty"))

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

result = (
    with_latest
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "unit_price",
        F.when(F.col("brand").rlike("^(Stark|Globex)$"), "GROUP_B").when(F.col("brand").rlike("^(Acme|Initech)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("brand_zone"),
        F.col("avg_product_stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc_nulls_last())
)
