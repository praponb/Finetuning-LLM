from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3400)
    .select(
        "store_id",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.weekofyear("sale_date").alias("sale_date_weekofyear"),
    )
)
