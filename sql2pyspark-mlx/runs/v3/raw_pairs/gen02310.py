from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter((F.col("p.category") == "Sports") & F.col("p.launch_date").between("2023-06-01", "2024-05-01"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy("country", "last_restock", "id")
    .limit(5)
)
