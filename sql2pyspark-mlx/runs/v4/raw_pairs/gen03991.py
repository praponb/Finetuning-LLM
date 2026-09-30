from pyspark.sql import functions as F

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.city"))
)

result = (
    joined
    .select(
        "id",
        "product_name",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "city",
    )
    .orderBy("supplier_id")
)
