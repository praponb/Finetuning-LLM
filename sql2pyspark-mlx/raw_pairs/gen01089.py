from pyspark.sql import functions as F

dim_totals = (
    df_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .cube(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.unit_price")), 2).alias("total_unit_price"),
    )
)

result = (
    dim_totals
    .select("total_unit_price")
    .orderBy("total_unit_price")
)
