from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 10]))
    .select(
        "id",
        "product_id",
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
    )
    .orderBy("product_id", "id")
    .limit(1)
)
