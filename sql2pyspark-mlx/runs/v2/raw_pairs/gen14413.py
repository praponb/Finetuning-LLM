from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("stock_qty").between(167, 299))
        .select("id", "supplier_id")
    )
)
