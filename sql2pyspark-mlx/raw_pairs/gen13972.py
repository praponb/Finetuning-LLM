from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("store_id").isin([1, 2]))

agg = (
    src
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    agg
    .filter(F.col("row_count") < 7)
    .select("product_id", "row_count", "latest_sale_date")
    .orderBy("product_id")
)
