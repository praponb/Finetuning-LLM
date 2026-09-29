from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("category") != "Fashion").alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_anti")
    .filter(~F.col("suppliers.country").isin(["UK", "SG", "JP"]))
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("suppliers.country"),
    )
)
