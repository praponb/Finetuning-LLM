from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "product_id",
        "revenue",
        "sale_date",
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
    )
    .orderBy("sale_date", F.col("sale_date_plus_months").desc())
)
