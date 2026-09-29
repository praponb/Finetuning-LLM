from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("price").alias("sum_product_price"))

latest_match = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

result = (
    latest_match
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "supplier_id",
        "order_item_unit_price",
        F.col("sum_product_price"),
    )
    .orderBy(F.col("supplier_id").desc_nulls_first())
)
