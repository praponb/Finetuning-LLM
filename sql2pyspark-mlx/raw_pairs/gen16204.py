from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("least_stock_qty_price", F.least("stock_qty", "price"))
)

result = (
    cleaned
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.category",
        "a.cost",
        "a.launch_date",
        "a.tags",
        "a.supplier_id",
        F.col("p.city"),
    )
)
