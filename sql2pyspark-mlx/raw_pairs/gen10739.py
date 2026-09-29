from pyspark.sql import functions as F

rollup_totals = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.quantity")).alias("max_quantity"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select("cnt", "max_quantity", "gid")
    .orderBy(F.col("gid").asc_nulls_first(), "max_quantity")
)
