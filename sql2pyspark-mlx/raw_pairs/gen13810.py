from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 8, 9]))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

result = (
    base
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "sale"))
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.units",
        "a.days_since_sale_date",
        F.col("p.product_name"),
    )
)
