from pyspark.sql import functions as F

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.rating"), F.col("p.city"), F.col("p.supplier_name"))
)

result = (
    joined
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        "city",
    )
    .orderBy("supplier_id")
)
