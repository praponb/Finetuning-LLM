from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select("launch_date")
    .unionByName(df_customers.select(F.col("signup_date").alias("launch_date")))
)
