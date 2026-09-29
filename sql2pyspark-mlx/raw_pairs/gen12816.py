from pyspark.sql import functions as F

cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.category",
        "a.brand",
        "a.price",
        "a.stock_qty",
        "a.launch_date",
        "a.tags",
        "a.supplier_id",
        F.col("s.supplier_name"),
    )
)

result = (
    joined
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("min_product_cost"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("launch_date").desc())
)
