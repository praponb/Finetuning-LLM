from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 350)
    .select("launch_date", "stock_qty")
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("launch_date", "stock_qty")
    )
    .distinct()
    .orderBy("launch_date")
)
