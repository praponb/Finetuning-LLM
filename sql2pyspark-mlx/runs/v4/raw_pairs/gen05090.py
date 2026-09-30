from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("units") < 36) | F.month("sale_date").isin([1, 2, 9])))
    .select(
        "id",
        "units",
        "sale_date",
        "revenue",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)
