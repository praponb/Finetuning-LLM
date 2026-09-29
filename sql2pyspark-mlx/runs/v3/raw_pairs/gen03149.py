from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["PENDING", "SHIPPED"]))
    .select("amount", F.lit("order").alias("source"))
    .union(
        df_products
        .filter(F.col("launch_date").between("2024-06-01", "2024-08-15"))
        .select("cost", F.lit("product").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
