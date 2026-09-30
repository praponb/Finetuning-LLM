from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter((F.col("stock_qty") <= 318) & (F.col("launch_date") > F.to_date(F.lit("2023-03-20"))))
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
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id") == 23), "left_anti")
)

joined = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.city").isNotNull())
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.tags",
        "s.supplier_id",
        "s.brand_title",
        "s.quantity",
        F.col("p2.city"),
    )
)

eligible = (
    joined
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 5, 6, 7])), "left_anti")
)

enriched = (
    eligible
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.rating"), F.col("s.city").alias("supplier_city"))
)

result = (
    enriched
    .select(
        "id",
        "cost",
        "tags",
        "brand_title",
        "quantity",
        "city",
        "rating",
        "supplier_city",
    )
    .orderBy(F.col("city").desc_nulls_last(), "supplier_city")
)
