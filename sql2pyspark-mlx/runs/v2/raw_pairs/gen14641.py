from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Acme")
    .select("supplier_id", "category")
    .unionByName(
        df_products
        .filter(F.col("stock_qty") >= 325)
        .select("supplier_id", "category")
    )
    .distinct()
)
