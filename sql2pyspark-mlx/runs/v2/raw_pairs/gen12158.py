from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 6]) & F.col("store_id").isin([3, 5]) & F.month("sale_date").isin([1, 3, 10]))
)

summary = (
    filtered_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.count("*").alias("num_store_sales"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    summary
    .filter(F.col("num_store_sales") < 9)
    .select("sale_month", "num_store_sales", "last_sale_date")
)
