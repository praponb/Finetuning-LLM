from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2025-10-01") & F.col("sale_date").between("2023-05-01", "2025-07-01"))
    .select("sale_date", "units", F.round("units", 0).alias("units_rounded"))
    .orderBy("sale_date")
)
