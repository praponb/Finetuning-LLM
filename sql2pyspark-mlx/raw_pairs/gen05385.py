from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter((F.col("stock_qty") <= 169) & F.array_contains("tags", "premium"))
)

summary = (
    valid_products
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    summary
    .select("brand", "min_cost")
    .orderBy(F.col("brand").desc())
)
