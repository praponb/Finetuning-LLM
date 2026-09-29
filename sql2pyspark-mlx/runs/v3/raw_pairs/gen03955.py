from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 178)
    .select("id", "supplier_id", "stock_qty")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([3, 6, 12]))
        .select("id", "supplier_id", "stock_qty")
    )
)
