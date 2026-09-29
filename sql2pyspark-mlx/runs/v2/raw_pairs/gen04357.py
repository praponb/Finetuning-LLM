from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter((F.col("sale_date") <= "2024-08-01") & F.col("sale_date").between("2023-04-01", "2024-09-04") & F.col("units").between(14, 16))
)

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    agg
    .filter(F.col("units_count") > 4)
    .orderBy("last_sale_date")
)
