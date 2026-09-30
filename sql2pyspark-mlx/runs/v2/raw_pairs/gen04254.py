from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .select(
        "id",
        "units",
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
        F.date_add("sale_date", 19).alias("sale_date_plus_19d"),
    )
)
