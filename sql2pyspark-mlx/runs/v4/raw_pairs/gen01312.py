from pyspark.sql import functions as F

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([5, 11, 25, 32])), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") >= F.col("s.launch_date")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.rating"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("cost") >= 730, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("cost_per_home"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows"),
    )
)
