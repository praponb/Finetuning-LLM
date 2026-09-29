from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNull() & F.col("sale_date").between("2023-06-01", "2025-03-01"))
    .select(
        "id",
        "revenue",
        F.round((F.col("units") * 0.9), 2).alias("units_adjusted"),
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
    .orderBy("id")
    .limit(10)
)
