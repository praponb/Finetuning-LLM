from pyspark.sql import functions as F

cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

remaining_keys = (
    df_products
    .select("supplier_id", "category")
    .exceptAll(
        df_products
        .filter(F.col("category") == "Electronics")
        .select("supplier_id", "category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("category").rlike("^(Fashion|Sports)$"), "GROUP_B").when(F.col("category").rlike("^(Electronics|Home)$"), "CORE").otherwise("EXTENDED").alias("category_zone"),
        F.col("avg_product_cost"),
    )
    .orderBy("id", "category")
)
