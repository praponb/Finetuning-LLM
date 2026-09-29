from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("stock_qty").alias("avg_product_stock_qty"))

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.supplier_name"), F.col("b.country"), F.col("b.city"))
)

kept = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isin([8, 39])), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

result = (
    with_latest
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "supplier_name",
        F.col("avg_product_stock_qty"),
    )
    .orderBy(F.col("launch_date").desc_nulls_first(), F.col("cost").desc())
)
