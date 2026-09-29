from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-01-01", "2025-08-01") & F.col("units").between(15, 26))
    .select(
        "id",
        "units",
        "store_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.round("units", 0).alias("units_rounded"),
    )
)
