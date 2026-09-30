from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("brand") != "Umbrella")
    .groupBy("supplier_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("num_products"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .filter(F.col("S.min_cost") > 690)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("S.min_cost"),
        F.col("S.num_products"),
    )
    .orderBy(F.col("id").desc(), "num_products")
)
