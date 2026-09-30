from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4]) & F.col("sale_date").between("2024-10-01", "2024-12-27"))
)

agg = (
    src
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.count("*").alias("row_count"),
    )
)

result = (
    agg
    .select("sale_month", "last_sale_date", "row_count")
    .orderBy("last_sale_date", "sale_month")
    .limit(50)
)
