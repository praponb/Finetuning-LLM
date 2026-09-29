from pyspark.sql import functions as F

src = df_store_sales.filter(F.month("sale_date").isin([1, 3, 4]))

agg = (
    src
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    agg
    .filter(F.col("num_store_sales") < 7)
    .select("product_id", "num_store_sales")
)
