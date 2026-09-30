from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-06-10").cast("date")) & F.col("supplier_id").isNotNull())
    .select("*", F.date_add("launch_date", 28).alias("launch_date_plus_28d"))
)

base = (
    filtered
    .filter(F.col("launch_date") >= F.lit("2023-06-23").cast("date"))
    .select(
        "id",
        "product_name",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "launch_date_plus_28d",
        F.least("price", "cost").alias("least_price_cost"),
        F.when(F.col("category").isin(["Home", "Books"]), "Group A").otherwise("Other").alias("category_group"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.launch_date")), "left_anti")
)

eligible_products = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 4, 6])), "left_anti")
)

result = (
    eligible_products
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "launch_date_plus_28d",
        "least_price_cost",
        "category_group",
        F.col("max_product_price"),
    )
    .orderBy(F.col("supplier_id").asc_nulls_last())
)
