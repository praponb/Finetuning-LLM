from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("a")
    .join(df_products.alias("dim"), F.col("a.product_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.quantity")).alias("max_quantity"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select(
        "category",
        "num_rows",
        "max_quantity",
        "gid",
        F.when(F.col("category").rlike("^(Books|Grocery)$"), "EXTENDED").otherwise("CORE").alias("category_zone"),
    )
    .orderBy("gid")
)
