from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(373, 382))
    .select("supplier_id")
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("supplier_id")
    )
    .distinct()
)
