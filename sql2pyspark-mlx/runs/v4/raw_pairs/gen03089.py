from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_last(), "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

filtered = (
    df_products
    .filter(F.col("cost") > 230)
    .withColumn("stock_qty_minus_price", (F.col("stock_qty") - F.col("price")))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("OTHER")))
)

eligible = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units") > 20), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Running Shoes|Smart Watch)$"), "EXTENDED").otherwise("PRIORITY").alias("product_name_zone"),
        F.col("avg_product_cost"),
    )
    .orderBy("id")
)
