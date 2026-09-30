from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.month("launch_date")).orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("launch_date"), "supplier_id")
w6 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("supplier_id", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("order_item_quantity", "id")

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units").alias("store_sale_units"),
    )
)

base = (
    latest_match
    .filter(F.col("brand").eqNullSafe(F.lit("Acme")))
    .select(
        "*",
        F.when(F.col("price") >= 1140, "High").when(F.col("price") >= 270, "Medium").otherwise("Low").alias("price_band"),
        F.array_contains("tags", "clearance").alias("is_clearance"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date_2"),
        F.col("c.units").alias("store_sale_units_2"),
    )
)

with_latest_products = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.cost",
        "s.launch_date",
        "s.supplier_id",
        "s.store_sale_units",
        "s.is_clearance",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

enriched = (
    with_latest_products
    .filter(F.col("store_sale_units") == 39)
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "store_sale_units",
        "order_item_quantity",
        F.first("store_sale_units", ignorenulls=True).over(w4).alias("first_known_store_sale_units"),
        F.min("store_sale_units").over(w5).alias("launch_date_year_min_store_sale_units"),
        F.sum("store_sale_units").over(w6).alias("running_store_sale_units"),
        F.sum("cost").over(w7).alias("cumulative_cost"),
        F.ntile(5).over(w8).alias("order_item_quantity_bucket"),
    )
)

with_latest_products_2 = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.sale_date").alias("store_sale_sale_date"))
)

report = (
    with_latest_products_2
    .select(
        "id",
        "category",
        "cost",
        "store_sale_units",
        "order_item_quantity",
        "first_known_store_sale_units",
        "launch_date_year_min_store_sale_units",
        "running_store_sale_units",
        "cumulative_cost",
    )
    .orderBy(F.col("order_item_quantity").asc_nulls_last())
)

result = (
    report
    .select(
        "category",
        "order_item_quantity",
        "first_known_store_sale_units",
        "launch_date_year_min_store_sale_units",
        "running_store_sale_units",
        "cumulative_cost",
        F.when(F.col("category").rlike("^(Home|Sports)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("category_zone"),
    )
)
