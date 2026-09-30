from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 6]) & (F.col("units") == 15))
    .select(
        "id",
        "units",
        "product_id",
        "sale_date",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
    .limit(5)
)
