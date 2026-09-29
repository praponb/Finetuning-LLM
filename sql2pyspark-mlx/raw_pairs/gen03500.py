from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select(
        "revenue",
        "store_id",
        "sale_date",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
        F.quarter("sale_date").alias("sale_date_quarter"),
    )
)
