from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.launch_date") > "2023-10-01")
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("city").desc())
)
