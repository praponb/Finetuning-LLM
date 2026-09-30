from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .unionByName(df_customers.select(F.col("signup_date").alias("sale_date")))
    .orderBy("sale_date")
)
