from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3]) & (F.col("revenue") >= 2000))
    .groupBy(
        "product_id",
        F.year("sale_date").alias("sale_year"),
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.countDistinct("units").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 3)
    .drop("unique_stores")
)
