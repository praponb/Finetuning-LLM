from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-07-31").cast("date")) & F.col("sale_date").between("2023-12-01", "2024-05-02"))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.product_name"), F.col("p.launch_date"), F.col("p.category"))
)
