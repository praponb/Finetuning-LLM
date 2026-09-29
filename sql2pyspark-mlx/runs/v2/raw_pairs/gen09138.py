from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "product_id",
        F.ceil("revenue").alias("revenue_whole"),
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)
