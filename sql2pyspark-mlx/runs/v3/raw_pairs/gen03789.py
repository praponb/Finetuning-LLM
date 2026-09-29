from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([3, 4, 5, 9]))
        .select(F.col("sale_date").alias("signup_date"))
    )
    .orderBy("signup_date")
)
