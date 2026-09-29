from pyspark.sql import functions as F

by_dimension = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .cube(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.units")), 2).alias("avg_units"),
        F.round(F.avg(F.col("e.revenue")), 2).alias("avg_revenue"),
    )
)

result = (
    by_dimension
    .select(
        "category",
        "rows_total",
        "avg_units",
        "avg_revenue",
        F.when(F.col("category").rlike("^(Books|Sports)$"), "GROUP_B").otherwise("GROUP_A").alias("category_zone"),
    )
    .orderBy("avg_revenue")
)
