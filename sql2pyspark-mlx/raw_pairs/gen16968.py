from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 14)
    .select("id", "supplier_id")
    .unionByName(
        df_products
        .filter(F.col("category") == "Electronics")
        .select("id", "supplier_id")
    )
)
