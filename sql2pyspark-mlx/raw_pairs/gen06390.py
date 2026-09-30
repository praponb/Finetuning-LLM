from pyspark.sql import functions as F

cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("cost").alias("sum_product_cost"))

kept_keys = (
    df_products
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("sum_product_cost"),
    )
    .orderBy(F.col("cost").desc_nulls_last(), F.col("price").desc())
)
