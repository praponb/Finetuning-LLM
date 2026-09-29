from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").asc_nulls_last(), "id")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

enriched = (
    with_latest
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id").eqNullSafe(F.col("s.id")), "left")
    .select("e.*", F.col("s.country"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "price", "tags", "supplier_id", "country")
)

with_dims = (
    latest
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "Berlin")
    .select("a.*", F.col("s.supplier_name"))
)

with_dims_products = (
    with_dims
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select(
        "s.*",
        F.col("p2.rating"),
        F.col("p2.country").alias("supplier_country"),
        F.col("p2.city"),
    )
)

eligible = (
    with_dims_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity").between(5, 6)), "left_anti")
)

result = (
    eligible
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "price",
        "supplier_id",
        "country",
        "supplier_name",
        "rating",
        "supplier_country",
        "city",
        F.col("sum_product_stock_qty"),
    )
)
