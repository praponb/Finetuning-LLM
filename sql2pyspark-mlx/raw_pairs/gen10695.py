from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select("signup_date", F.lit("customer").alias("source"))
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").isNotNull())
        .select(
        F.col("sale_date").alias("signup_date"),
        F.lit("store_sale").alias("source"),
    )
    )
    .distinct()
)
