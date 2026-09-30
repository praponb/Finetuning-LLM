from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5]) & F.col("store_id").isin([1, 3, 4, 5]) & (F.col("units") != 12))
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.when(F.col("units") >= 44, "High").when(F.col("units") >= 21, "Medium").otherwise("Low").alias("units_level"),
        F.date_trunc("month", "sale_date").alias("sale_date_period"),
    )
)
