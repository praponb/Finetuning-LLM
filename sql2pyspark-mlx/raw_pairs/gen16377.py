from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") >= 364), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
    )
    .orderBy("london_rating")
)
