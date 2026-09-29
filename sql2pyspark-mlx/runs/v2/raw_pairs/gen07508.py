from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "sale_date",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)
