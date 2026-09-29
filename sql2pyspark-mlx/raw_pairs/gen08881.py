from pyspark.sql import functions as F

rollup_totals = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category").alias("category"), F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.quantity")), 2).alias("total_quantity"),
    )
)

result = (
    rollup_totals
    .select(
        "category",
        "brand",
        "rows_total",
        F.when(F.col("brand").rlike("^(Stark|Initech)$"), "GROUP_A").otherwise("GROUP_B").alias("brand_zone"),
    )
    .orderBy(F.col("brand").desc(), F.col("category").desc())
)
