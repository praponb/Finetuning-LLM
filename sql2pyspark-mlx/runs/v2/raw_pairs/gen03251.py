from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.cost") > 240)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy("id")
)
