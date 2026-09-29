from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") <= "2025-07-20") & ((F.col("units") < 23) | (F.col("units") < 15)))
    .select(
        "id",
        "revenue",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)
