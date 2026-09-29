from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3400) & (F.col("revenue") >= 1300) & F.col("units").between(12, 15))
    .select(
        "id",
        "units",
        "store_id",
        "sale_date",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy("sale_date", "id")
    .limit(10)
)
