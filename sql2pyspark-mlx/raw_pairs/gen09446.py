from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.avg_units") > 30)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("pt.avg_units"),
    )
)
