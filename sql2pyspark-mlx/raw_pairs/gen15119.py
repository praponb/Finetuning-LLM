from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 318)
    .select("id", "supplier_id", "stock_qty")
    .subtract(
        df_products
        .filter(F.col("cost") >= 410)
        .select("id", "supplier_id", "stock_qty")
    )
)
