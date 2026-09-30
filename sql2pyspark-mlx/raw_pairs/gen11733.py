from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 12)
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
    .filter(F.col("stock_qty_range") < 68)
    .orderBy("stock_qty_range", "supplier_id")
)
