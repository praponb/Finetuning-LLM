from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter((F.col("s.rating") >= 3.8) & F.col("p.cost").between(250, 570))
    .select(
        F.col("s.country"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)
