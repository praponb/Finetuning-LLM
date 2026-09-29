from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .filter(F.array_contains(F.col("p.tags"), "sale") & (F.col("ss.revenue") < 1500))
    .select(
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("p.brand"),
        F.col("p.cost"),
    )
    .orderBy(F.col("units").desc(), F.col("revenue").desc())
)
