from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("price")).otherwise(0)).alias("home_price"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("P.home_price"),
        F.col("P.latest_launch_date"),
    )
)
