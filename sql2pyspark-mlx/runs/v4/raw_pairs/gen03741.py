from pyspark.sql import functions as F

joined = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"))
)

result = (
    joined
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        "country",
    )
)
