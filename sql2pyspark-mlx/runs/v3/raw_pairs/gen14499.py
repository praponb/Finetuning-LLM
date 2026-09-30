from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~((F.col("p.stock_qty") != 144) & (F.col("p.launch_date") > "2025-03-01")))
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("country").desc(), "launch_date")
)
