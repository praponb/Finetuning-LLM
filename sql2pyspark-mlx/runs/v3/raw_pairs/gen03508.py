from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.col("city") != "Singapore").select("id")

result = (
    df_products.alias("p")
    .join(excluded_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.col("p.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .filter(F.col("p.cost").between(330, 690))
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
    )
)
