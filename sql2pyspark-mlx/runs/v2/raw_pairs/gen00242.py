from pyspark.sql import functions as F

base = (
    df_products
    .filter(~((F.length("product_name") < 8) & (F.col("category") == "Home")))
)

agg = (
    base
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = agg.filter(F.col("stock_qty_range") >= 66)
