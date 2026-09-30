from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
        F.date_sub("launch_date", 43).alias("launch_date_minus_43d"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.launch_date_minus_43d")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

enriched = (
    with_last
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.brand",
        "e.tags",
        "e.supplier_id",
        "e.product_name_normalized",
        "e.launch_date_minus_43d",
        "e.row_num",
        "e.quantity",
        "e.order_item_unit_price",
        F.col("d.country"),
        F.col("d.city"),
        F.col("d.supplier_name"),
    )
)

result = (
    enriched
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "brand",
        "tags",
        "supplier_id",
        "product_name_normalized",
        "launch_date_minus_43d",
        "row_num",
        "quantity",
        "order_item_unit_price",
        F.col("min_product_cost"),
    )
)
